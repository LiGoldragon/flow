//! Curriculum 478c's own frames. A Curriculum Nexus built at 478cbc5 ran on
//! frozen context, and its own CLI asked it twice through a proxy that kept
//! each frame raw: a ComposeStartup naming a module it lacks, answered
//! `Rejected` (the fifth Outcome variant of its schema), and one it
//! composes, answered `StartupComposed`. Flow asks as that CLI asks, byte
//! for byte; its schema reads each answer as written; and its production
//! composer, given each answer on a socket, refuses the first with
//! Curriculum's text kept whole and verifies the second.

use crate::context::{
    CONTEXT_OPENING, ComposesFlowContext, ContextRequest, CurriculumComposer,
    OpensCurriculumComposer, SelectsStartup, VerifiedContext, VerifiesContext,
};
use crate::generated::curriculum::{Outcome, Query};
use signal_flow::{Address, Aspect, ContextRefusal, HarnessKind, Model};
use std::io::{Read, Write};

/// `ComposeStartup.{ { Psyche core Fable } Claude [ spirit:core ]
/// [ knowledge:no-such-module ] [] }`, as Curriculum's CLI sent it.
const MISSING_MODULE_QUERY: &[u8] = include_bytes!("curriculum-478c/missing-module.query.rkyv");
/// Its answer: `Rejected.«missing module knowledge:no-such-module»`.
const MISSING_MODULE_ANSWER: &[u8] = include_bytes!("curriculum-478c/missing-module.answer.rkyv");
/// `ComposeStartup.{ { Psyche book Haiku } Claude [ spirit:core
/// intent:energy ] [] [] }`, as Curriculum's CLI sent it.
const STARTUP_COMPOSED_QUERY: &[u8] = include_bytes!("curriculum-478c/startup-composed.query.rkyv");
/// Its answer: the composed startup.
const STARTUP_COMPOSED_ANSWER: &[u8] =
    include_bytes!("curriculum-478c/startup-composed.answer.rkyv");

fn request(address: Address, system: &[&str], user: &[&str]) -> ContextRequest {
    ContextRequest {
        address,
        harness_kind: HarnessKind::Claude,
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
            aspect: Aspect::Psyche,
            topic: flow_ethos::Topic("core".into()),
            model: Model::Fable,
        },
        &["spirit:core"],
        &["knowledge:no-such-module"],
    )
}

fn startup_request() -> ContextRequest {
    request(
        Address {
            aspect: Aspect::Psyche,
            topic: flow_ethos::Topic("book".into()),
            model: Model::Haiku,
        },
        &["spirit:core", "intent:energy"],
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

/// A real 478c `Rejected` frame reads as `Rejected` with its text, never as
/// another variant, and the production composer refuses the request with
/// `ComposerRejected` carrying that text whole.
#[test]
fn a_real_478c_rejected_frame_is_a_composer_rejection_carrying_its_text() {
    let request = missing_module_request();
    assert_eq!(
        rkyv::to_bytes::<rkyv::rancor::Error>(&Query::ComposeStartup(request.startup_selection()))
            .expect("query archives")
            .as_slice(),
        MISSING_MODULE_QUERY,
        "Flow asks as Curriculum's own CLI asks"
    );
    assert_eq!(
        rkyv::from_bytes::<Outcome, rkyv::rancor::Error>(MISSING_MODULE_ANSWER)
            .expect("the 478c answer reads"),
        Outcome::Rejected("missing module knowledge:no-such-module".into())
    );
    let (composer, server) = replayed(MISSING_MODULE_ANSWER);
    assert_eq!(
        composer.compose_flow_context(&request),
        Err(ContextRefusal::ComposerRejected(
            "missing module knowledge:no-such-module".into()
        ))
    );
    assert_eq!(server.join().expect("served"), MISSING_MODULE_QUERY);
}

/// A real 478c `StartupComposed` frame reads as the composition it is, and
/// the production composer's answer passes Flow's verification: its
/// payload digests, its modules at their placements, its rendering.
#[test]
fn a_real_478c_startup_composed_frame_reads_and_verifies() {
    let request = startup_request();
    assert_eq!(
        rkyv::to_bytes::<rkyv::rancor::Error>(&Query::ComposeStartup(request.startup_selection()))
            .expect("query archives")
            .as_slice(),
        STARTUP_COMPOSED_QUERY,
        "Flow asks as Curriculum's own CLI asks"
    );
    let Outcome::StartupComposed(composition) =
        rkyv::from_bytes::<Outcome, rkyv::rancor::Error>(STARTUP_COMPOSED_ANSWER)
            .expect("the 478c answer reads")
    else {
        panic!("a StartupComposed frame")
    };
    assert!(composition.system_text.0.starts_with(CONTEXT_OPENING));
    assert_eq!(
        composition
            .context_provenance_vector
            .iter()
            .map(|provenance| provenance.module_identity.0.as_str())
            .collect::<Vec<_>>(),
        ["spirit:core", "intent:energy"]
    );
    let (composer, server) = replayed(STARTUP_COMPOSED_ANSWER);
    let composed = composer
        .compose_flow_context(&request)
        .expect("the composer takes the composition");
    assert_eq!(composed, composition);
    assert!(VerifiedContext::verified(&request, &composed).is_ok());
    assert_eq!(server.join().expect("served"), STARTUP_COMPOSED_QUERY);
}
