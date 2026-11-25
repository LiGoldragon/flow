use flow_rewrite::memory::{
    ConfigurationEffect, OpensPermaflowMemory, PermaflowMemory, ReadPermaflow,
};
use flow_rewrite::{
    ClaudeLaunch, Emission, EmissionOutcome, Frame, Framed, Input, MetaInput, MetaOutput, Output,
    Refusal,
};
use kameo::prelude::*;
use rkyv::rancor::Error;
use std::{
    io,
    path::{Path, PathBuf},
};
use tokio::net::{UnixListener, UnixStream};

struct PrivatePlace(PathBuf);

trait ServesFlow {
    fn serve(&self) -> impl Future<Output = io::Result<()>>;
}

impl ServesFlow for PrivatePlace {
    async fn serve(&self) -> io::Result<()> {
        let memory = PermaflowMemory::open(&self.0.join("flow.sema")).map_err(io::Error::other)?;
        let meta = UnixListener::bind(self.0.join("flow-meta.sock"))?;
        let ordinary = UnixListener::bind(self.0.join("flow.sock"))?;
        let emission = self.0.join("flow-emission.sock");
        let memory = PermaflowMemory::spawn(memory);
        loop {
            tokio::select! {
                connection = meta.accept() => {
                    let mut stream = connection?.0;
                    let memory = memory.clone();
                    tokio::spawn(async move { stream.answer_meta(memory).await });
                }
                connection = ordinary.accept() => {
                    let mut stream = connection?.0;
                    let memory = memory.clone();
                    let emission = emission.clone();
                    tokio::spawn(async move { stream.answer_ordinary(memory, emission).await });
                }
            }
        }
    }
}

trait AnswersSignal {
    fn answer_meta(
        &mut self,
        memory: ActorRef<PermaflowMemory>,
    ) -> impl Future<Output = io::Result<()>> + Send;
    fn answer_ordinary(
        &mut self,
        memory: ActorRef<PermaflowMemory>,
        emission: PathBuf,
    ) -> impl Future<Output = io::Result<()>> + Send;
}

impl AnswersSignal for UnixStream {
    async fn answer_meta(&mut self, memory: ActorRef<PermaflowMemory>) -> io::Result<()> {
        let Frame(bytes) = Frame::read_async(self).await?;
        let input = rkyv::from_bytes::<MetaInput, Error>(&bytes).map_err(io::Error::other)?;
        let output = match memory.ask(input).await {
            Ok((ConfigurationEffect::Committed, observation)) => MetaOutput::Committed(observation),
            Ok((ConfigurationEffect::Unchanged, observation)) => MetaOutput::Unchanged(observation),
            Ok((ConfigurationEffect::Observed, observation)) => {
                MetaOutput::Configuration(observation)
            }
            Err(kameo::error::SendError::HandlerError(refusal)) => MetaOutput::Refused(refusal),
            Err(_) => MetaOutput::Refused(Refusal::ActorUnavailable),
        };
        Frame(
            rkyv::to_bytes::<Error>(&output)
                .map_err(io::Error::other)?
                .to_vec(),
        )
        .write_async(self)
        .await
    }

    async fn answer_ordinary(
        &mut self,
        memory: ActorRef<PermaflowMemory>,
        emission: PathBuf,
    ) -> io::Result<()> {
        let Frame(bytes) = Frame::read_async(self).await?;
        let Input::Launch(address) =
            rkyv::from_bytes::<Input, Error>(&bytes).map_err(io::Error::other)?;
        let output = match memory.ask(ReadPermaflow(address)).await {
            Ok(configuration) => {
                emission
                    .as_path()
                    .emit(Emission::LaunchClaude(ClaudeLaunch { configuration }))
                    .await
            }
            Err(kameo::error::SendError::HandlerError(refusal)) => Output::Refused(refusal),
            Err(_) => Output::Refused(Refusal::ActorUnavailable),
        };
        Frame(
            rkyv::to_bytes::<Error>(&output)
                .map_err(io::Error::other)?
                .to_vec(),
        )
        .write_async(self)
        .await
    }
}

trait EmitsLaunch {
    fn emit(&self, emission: Emission) -> impl Future<Output = Output> + Send;
}

impl EmitsLaunch for Path {
    async fn emit(&self, emission: Emission) -> Output {
        let Ok(bytes) = rkyv::to_bytes::<Error>(&emission) else {
            return Output::EmissionUnsubmitted;
        };
        let Ok(mut socket) = UnixStream::connect(self).await else {
            return Output::EmissionUnsubmitted;
        };
        if Frame(bytes.to_vec())
            .write_async(&mut socket)
            .await
            .is_err()
        {
            return Output::EmissionUnanswered;
        }
        let Ok(Frame(bytes)) = Frame::read_async(&mut socket).await else {
            return Output::EmissionUnanswered;
        };
        match rkyv::from_bytes::<EmissionOutcome, Error>(&bytes) {
            Ok(outcome) => Output::Launched(outcome),
            Err(_) => Output::EmissionUnanswered,
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let place = std::env::args_os()
        .nth(1)
        .ok_or(io::ErrorKind::InvalidInput)?;
    PrivatePlace(PathBuf::from(place)).serve().await
}
