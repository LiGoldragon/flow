//! The first private Flow slice: one persisted permaflow setting and its launch projection.
use rkyv::{Archive, Deserialize, Serialize};
use signal_flow::Address;
use std::io::{self, Read, Write};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub mod memory;

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct HerdrExecutable(pub String);
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct HerdrSessionName(pub String);
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ClaudeModelName(pub String);
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ClaudeEffort(pub String);
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct HerdrPaneId(pub String);
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct HerdrAgentName(pub String);

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ClaudeLaunchTarget {
    pub herdr_executable: HerdrExecutable,
    pub herdr_session_name: HerdrSessionName,
    pub claude_model_name: ClaudeModelName,
    pub claude_effort: ClaudeEffort,
    pub herdr_pane_id: HerdrPaneId,
    pub herdr_agent_name: HerdrAgentName,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum AgentToolAccess {
    Available,
    Excluded,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Permaflow {
    pub address: Address,
    pub claude_launch_target: ClaudeLaunchTarget,
    pub agent_tool_access: AgentToolAccess,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub enum PermaflowEdit {
    Define(Permaflow),
    AddAgentToolLeaveOut(Address),
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub enum MetaInput {
    Configure(PermaflowEdit),
    Configuration(Address),
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    pub sequence: u64,
    pub snapshot: u64,
    pub operation_count: u64,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ConfigurationObservation {
    pub permaflow: Permaflow,
    pub commit: Commit,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub enum Refusal {
    AlreadyDefined,
    UnknownPermaflow,
    CorruptMemory,
    MemoryUnavailable,
    ActorUnavailable,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub enum MetaOutput {
    Committed(ConfigurationObservation),
    Unchanged(ConfigurationObservation),
    Configuration(ConfigurationObservation),
    Refused(Refusal),
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub enum Input {
    Launch(Address),
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct ClaudeLaunch {
    pub configuration: ConfigurationObservation,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub enum Emission {
    LaunchClaude(ClaudeLaunch),
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub enum EmissionOutcome {
    Spawned { herdr_status: i32 },
    Refused { herdr_status: i32 },
    Unstarted,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub enum Output {
    Launched(EmissionOutcome),
    Refused(Refusal),
    EmissionUnsubmitted,
    EmissionUnanswered,
}

/// Transport framing is shared; the boundary chooses the archived type.
pub struct Frame(pub Vec<u8>);

pub trait Framed: Sized {
    fn read(stream: &mut impl Read) -> io::Result<Self>;
    fn write(&self, stream: &mut impl Write) -> io::Result<()>;
    fn read_async(
        stream: &mut (impl AsyncReadExt + Unpin),
    ) -> impl Future<Output = io::Result<Self>>;
    fn write_async(
        &self,
        stream: &mut (impl AsyncWriteExt + Unpin),
    ) -> impl Future<Output = io::Result<()>>;
}

impl Frame {
    const LIMIT: usize = 64 * 1024;

    fn length(bytes: [u8; 4]) -> io::Result<usize> {
        let length = u32::from_be_bytes(bytes) as usize;
        if length > Self::LIMIT {
            return Err(io::ErrorKind::InvalidData.into());
        }
        Ok(length)
    }

    fn prefix(&self) -> io::Result<[u8; 4]> {
        let size = u32::try_from(self.0.len()).map_err(io::Error::other)?;
        let prefix = size.to_be_bytes();
        Self::length(prefix)?;
        Ok(prefix)
    }
}

impl Framed for Frame {
    fn read(stream: &mut impl Read) -> io::Result<Self> {
        let mut length = [0; 4];
        stream.read_exact(&mut length)?;
        let mut archive = vec![0; Self::length(length)?];
        stream.read_exact(&mut archive)?;
        Ok(Self(archive))
    }

    fn write(&self, stream: &mut impl Write) -> io::Result<()> {
        stream.write_all(&self.prefix()?)?;
        stream.write_all(&self.0)
    }

    async fn read_async(stream: &mut (impl AsyncReadExt + Unpin)) -> io::Result<Self> {
        let mut length = [0; 4];
        stream.read_exact(&mut length).await?;
        let mut archive = vec![0; Self::length(length)?];
        stream.read_exact(&mut archive).await?;
        Ok(Self(archive))
    }

    async fn write_async(&self, stream: &mut (impl AsyncWriteExt + Unpin)) -> io::Result<()> {
        stream.write_all(&self.prefix()?).await?;
        stream.write_all(&self.0).await
    }
}
