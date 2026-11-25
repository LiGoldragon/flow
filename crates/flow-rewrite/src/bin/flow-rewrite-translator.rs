use flow_rewrite::{AgentToolAccess, ClaudeLaunch, Emission, EmissionOutcome, Frame, Framed};
use rkyv::rancor::Error;
use std::{
    io,
    os::unix::net::{UnixListener, UnixStream},
    path::PathBuf,
    process::Command,
};

trait Emittable {
    fn emit(self) -> EmissionOutcome;
}

impl Emittable for ClaudeLaunch {
    fn emit(self) -> EmissionOutcome {
        let observation = self.configuration;
        let permaflow = observation.permaflow;
        let target = permaflow.claude_launch_target;
        let mut command = Command::new(&target.herdr_executable.0);
        command.args([
            "--session",
            &target.herdr_session_name.0,
            "agent",
            "start",
            &target.herdr_agent_name.0,
            "--kind",
            "claude",
            "--pane",
            &target.herdr_pane_id.0,
            "--",
            "--model",
            &target.claude_model_name.0,
            "--effort",
            &target.claude_effort.0,
        ]);
        match permaflow.agent_tool_access {
            AgentToolAccess::Available => {}
            AgentToolAccess::Excluded => {
                command.args(["--disallowedTools", "Agent"]);
            }
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                eprintln!("Herdr did not start: {error}");
                return EmissionOutcome::Unstarted;
            }
        };
        eprintln!(
            "Launch request {:?} commit {:?} child {} argv {command:?}",
            permaflow.address,
            observation.commit,
            child.id()
        );
        match child.wait() {
            Ok(status) if status.success() => EmissionOutcome::Spawned { herdr_status: 0 },
            Ok(status) => EmissionOutcome::Refused {
                herdr_status: status.code().unwrap_or(-1),
            },
            Err(error) => {
                eprintln!("Herdr result not observed: {error}");
                EmissionOutcome::Unstarted
            }
        }
    }
}

trait TranslatesEmission {
    fn translate(&mut self) -> io::Result<()>;
}

impl TranslatesEmission for UnixStream {
    fn translate(&mut self) -> io::Result<()> {
        let Frame(bytes) = Frame::read(self)?;
        let emission = rkyv::from_bytes::<Emission, Error>(&bytes).map_err(io::Error::other)?;
        let outcome = match emission {
            Emission::LaunchClaude(launch) => launch.emit(),
        };
        Frame(
            rkyv::to_bytes::<Error>(&outcome)
                .map_err(io::Error::other)?
                .to_vec(),
        )
        .write(self)
    }
}

fn main() -> io::Result<()> {
    let place = std::env::args_os()
        .nth(1)
        .ok_or(io::ErrorKind::InvalidInput)?;
    let listener = UnixListener::bind(PathBuf::from(place).join("flow-emission.sock"))?;
    for connection in listener.incoming() {
        if let Err(error) = connection?.translate() {
            eprintln!("Invalid emission exchange: {error}");
        }
    }
    Ok(())
}
