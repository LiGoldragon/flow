use flow_ethos::Topic;
use flow_rewrite::{
    AgentToolAccess, ClaudeEffort, ClaudeLaunchTarget, ClaudeModelName, Frame, Framed,
    HerdrAgentName, HerdrExecutable, HerdrPaneId, HerdrSessionName, Input, MetaInput, MetaOutput,
    Output, Permaflow, PermaflowEdit,
};
use rkyv::rancor::Error;
use signal_flow::{Address, Aspect, Model};
use std::{io, os::unix::net::UnixStream, path::Path};

struct Witness<'a>(&'a Path);

trait ExchangesSignal {
    fn meta(&self, input: MetaInput) -> io::Result<MetaOutput>;
    fn launch(&self, address: Address) -> io::Result<Output>;
}

impl ExchangesSignal for Witness<'_> {
    fn meta(&self, input: MetaInput) -> io::Result<MetaOutput> {
        let mut stream = UnixStream::connect(self.0.join("flow-meta.sock"))?;
        Frame(
            rkyv::to_bytes::<Error>(&input)
                .map_err(io::Error::other)?
                .to_vec(),
        )
        .write(&mut stream)?;
        let Frame(reply) = Frame::read(&mut stream)?;
        rkyv::from_bytes::<MetaOutput, Error>(&reply).map_err(io::Error::other)
    }

    fn launch(&self, address: Address) -> io::Result<Output> {
        let mut stream = UnixStream::connect(self.0.join("flow.sock"))?;
        Frame(
            rkyv::to_bytes::<Error>(&Input::Launch(address))
                .map_err(io::Error::other)?
                .to_vec(),
        )
        .write(&mut stream)?;
        let Frame(reply) = Frame::read(&mut stream)?;
        rkyv::from_bytes::<Output, Error>(&reply).map_err(io::Error::other)
    }
}

fn main() -> io::Result<()> {
    let mut arguments = std::env::args().skip(1);
    let command = arguments.next().ok_or(io::ErrorKind::InvalidInput)?;
    let place = arguments.next().ok_or(io::ErrorKind::InvalidInput)?;
    let witness = Witness(Path::new(&place));
    let address = Address {
        aspect: Aspect::Mind,
        topic: Topic("agentToolWitness".into()),
        model: Model::Haiku,
    };
    let mut argument = || arguments.next().ok_or(io::ErrorKind::InvalidInput);
    match command.as_str() {
        "define" => {
            let target = ClaudeLaunchTarget {
                herdr_executable: HerdrExecutable(argument()?),
                herdr_session_name: HerdrSessionName(argument()?),
                herdr_pane_id: HerdrPaneId(argument()?),
                herdr_agent_name: HerdrAgentName("agent-tool-witness".into()),
                claude_model_name: ClaudeModelName("haiku".into()),
                claude_effort: ClaudeEffort("low".into()),
            };
            let outcome = witness.meta(MetaInput::Configure(PermaflowEdit::Define(Permaflow {
                address,
                claude_launch_target: target,
                agent_tool_access: AgentToolAccess::Available,
            })))?;
            println!("{outcome:?}");
            if !matches!(outcome, MetaOutput::Committed(_)) {
                return Err(io::Error::other("definition did not commit"));
            }
        }
        "configure" => {
            let outcome = witness.meta(MetaInput::Configure(
                PermaflowEdit::AddAgentToolLeaveOut(address),
            ))?;
            println!("{outcome:?}");
            if !matches!(outcome, MetaOutput::Committed(_)) {
                return Err(io::Error::other("amendment did not commit"));
            }
        }
        "configuration" => println!("{:?}", witness.meta(MetaInput::Configuration(address))?),
        "launch" => println!("{:?}", witness.launch(address)?),
        _ => return Err(io::ErrorKind::InvalidInput.into()),
    }
    Ok(())
}
