//! Flow Nexus owns dispatch and identity. Its ordinary and meta transports
//! carry rkyv archives; JSON below is only the external Codex app-server RPC.
pub mod codex;
pub mod store;
use codex::{CodexAdapter, ResumesCodex};
use signal_flow::{Query, Response};
use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::Path,
};
use store::{
    AuthorizesFlowRestart, ConfiguresFlowStore, ConfirmsStartedFlow, FlowStore, OpensFlowStore,
    RecordsPendingThread, RecordsRestartedFlow, ReservesPendingStart,
};
pub trait Applies {
    fn apply(&mut self, query: Query) -> Response;
}
pub struct RunningNexus {
    pub store: FlowStore,
    pub codex: CodexAdapter,
}
pub trait Dispatches {
    fn dispatch(&self, query: Query) -> Response;
}
impl Dispatches for RunningNexus {
    fn dispatch(&self, query: Query) -> Response {
        match query {
            Query::Start {
                flow_type,
                goal,
                origin,
            } => {
                let request = Query::Start {
                    flow_type,
                    goal,
                    origin,
                };
                let Query::Start { goal, origin, .. } = &request else {
                    unreachable!()
                };
                let goal = goal.clone();
                let origin = origin.clone();
                let Ok(Some(pending)) = self.store.reserve_pending_start(request) else {
                    return Response::StartRejected;
                };
                match self.codex.start_codex_observed(&goal, &origin, |thread| {
                    if self
                        .store
                        .record_pending_thread(&pending, thread.into())
                        .unwrap_or(false)
                    {
                        Ok(())
                    } else {
                        Err(codex::CodexAdapterUnavailable::Protocol(
                            "pending thread persistence failed".into(),
                        ))
                    }
                }) {
                    Ok(_thread) => self
                        .store
                        .confirm_started(&pending.flow_id)
                        .unwrap_or(Response::StartRejected),
                    _ => Response::StartRejected,
                }
            }
            Query::Restart {
                flow_id,
                authority_flow_id,
            } => match self.store.authorize_restart(&flow_id, &authority_flow_id) {
                Ok(Some(token)) => {
                    let origin = signal_flow::Origin {
                        parent_flow_id: token.authority_flow_id.clone(),
                        session: token.thread_id.clone(),
                        turn: "restart".into(),
                    };
                    if self
                        .codex
                        .resume_codex(&token.thread_id, "Resume this Flow.", &origin)
                        .is_ok()
                    {
                        self.store
                            .record_restarted(token)
                            .unwrap_or(Response::RestartRejected)
                    } else {
                        Response::RestartRejected
                    }
                }
                _ => Response::RestartRejected,
            },
        }
    }
}
pub trait OpensRunningNexus {
    fn open(
        store: &Path,
        socket: String,
        model: String,
        timeout: std::time::Duration,
    ) -> Result<Self, store::StoreError>
    where
        Self: Sized;
}
impl OpensRunningNexus for RunningNexus {
    fn open(
        store: &Path,
        socket: String,
        model: String,
        timeout: std::time::Duration,
    ) -> Result<Self, store::StoreError> {
        Ok(Self {
            store: FlowStore::open(store)?,
            codex: CodexAdapter {
                socket,
                model,
                timeout,
            },
        })
    }
}
pub trait ServesOrdinary {
    fn serve_ordinary(&self, socket: &Path) -> Result<(), String>;
}
impl ServesOrdinary for RunningNexus {
    fn serve_ordinary(&self, socket: &Path) -> Result<(), String> {
        let _ = fs::remove_file(socket);
        let listener = UnixListener::bind(socket).map_err(|e| e.to_string())?;
        fs::set_permissions(socket, fs::Permissions::from_mode(0o600))
            .map_err(|e| e.to_string())?;
        loop {
            let (mut peer, _) = listener.accept().map_err(|e| e.to_string())?;
            let reply = self.dispatch(Frame::read_query(&mut peer)?);
            Frame::write_response(&mut peer, &reply)?
        }
    }
}
pub trait ServesMeta {
    fn serve_meta(&self, socket: &Path) -> Result<(), String>;
}
impl ServesMeta for RunningNexus {
    fn serve_meta(&self, socket: &Path) -> Result<(), String> {
        let _ = fs::remove_file(socket);
        let listener = UnixListener::bind(socket).map_err(|e| e.to_string())?;
        fs::set_permissions(socket, fs::Permissions::from_mode(0o600))
            .map_err(|e| e.to_string())?;
        loop {
            let (mut peer, _) = listener.accept().map_err(|e| e.to_string())?;
            let query = Frame::read_meta_query(&mut peer)?;
            let meta_signal_flow::Query::Configure(configuration) = query;
            self.store
                .configure(configuration.clone())
                .map_err(|e| e.to_string())?;
            Frame::write_meta_response(
                &mut peer,
                &meta_signal_flow::Response::Configured {
                    configuration,
                    activation: meta_signal_flow::ConfigurationActivation::NexusRestartRequired,
                },
            )?
        }
    }
}
#[derive(Default)]
pub struct NexusCore {
    flows: HashMap<String, (String, u64)>,
}
pub trait Persists {
    fn persist(&self, path: &Path) -> std::io::Result<()>;
}
impl Persists for NexusCore {
    fn persist(&self, path: &Path) -> std::io::Result<()> {
        let entries = self
            .flows
            .iter()
            .map(|(id, (owner, g))| format!("{id}\t{owner}\t{g}\n"))
            .collect::<String>();
        fs::write(path, entries)
    }
}
impl Applies for NexusCore {
    fn apply(&mut self, query: Query) -> Response {
        match query {
            Query::Start {
                flow_type,
                goal: _,
                origin,
            } if flow_type == "codex-medium" => {
                let id = format!("flow-{:x}", self.flows.len() + 1);
                self.flows
                    .insert(id.clone(), (origin.parent_flow_id.clone(), 1));
                Response::Started {
                    flow_id: id,
                    origin,
                }
            }
            Query::Start { .. } => Response::StartRejected,
            Query::Restart {
                flow_id,
                authority_flow_id,
            } => match self.flows.get_mut(&flow_id) {
                Some((owner, g)) if *owner == authority_flow_id => {
                    *g += 1;
                    Response::Restarted {
                        flow_id,
                        generation: *g,
                    }
                }
                Some(_) | None => Response::RestartRejected,
            },
        }
    }
}
pub trait ServesSignal {
    fn serve_once(&mut self, socket: &Path) -> Result<(), String>;
    fn serve(&mut self, socket: &Path) -> Result<(), String>;
}
impl ServesSignal for NexusCore {
    fn serve_once(&mut self, socket: &Path) -> Result<(), String> {
        let _ = fs::remove_file(socket);
        let listener = UnixListener::bind(socket).map_err(|e| e.to_string())?;
        let (mut peer, _) = listener.accept().map_err(|e| e.to_string())?;
        let query = Frame::read_query(&mut peer)?;
        let response = self.apply(query);
        Frame::write_response(&mut peer, &response)
    }
    fn serve(&mut self, socket: &Path) -> Result<(), String> {
        loop {
            self.serve_once(socket)?;
        }
    }
}
pub struct Frame;
impl Frame {
    fn write_bytes(peer: &mut UnixStream, bytes: &[u8]) -> Result<(), String> {
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        peer.write_all(&bytes).map_err(|e| e.to_string())
    }
    fn read_bytes(peer: &mut UnixStream) -> Result<Vec<u8>, String> {
        let mut length = [0; 4];
        peer.read_exact(&mut length).map_err(|e| e.to_string())?;
        let mut bytes = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut bytes).map_err(|e| e.to_string())?;
        Ok(bytes)
    }
    pub fn write_query(peer: &mut UnixStream, value: &Query) -> Result<(), String> {
        Self::write_bytes(
            peer,
            &rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?,
        )
    }
    pub fn read_query(peer: &mut UnixStream) -> Result<Query, String> {
        rkyv::from_bytes::<Query, rkyv::rancor::Error>(&Self::read_bytes(peer)?)
            .map_err(|e| e.to_string())
    }
    pub fn write_response(peer: &mut UnixStream, value: &Response) -> Result<(), String> {
        Self::write_bytes(
            peer,
            &rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?,
        )
    }
    pub fn read_response(peer: &mut UnixStream) -> Result<Response, String> {
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&Self::read_bytes(peer)?)
            .map_err(|e| e.to_string())
    }
    pub fn read_meta_query(peer: &mut UnixStream) -> Result<meta_signal_flow::Query, String> {
        rkyv::from_bytes::<meta_signal_flow::Query, rkyv::rancor::Error>(&Self::read_bytes(peer)?)
            .map_err(|e| e.to_string())
    }
    pub fn write_meta_response(
        peer: &mut UnixStream,
        value: &meta_signal_flow::Response,
    ) -> Result<(), String> {
        Self::write_bytes(
            peer,
            &rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_flow::Origin;
    use std::{thread, time::Duration};
    #[test]
    fn matching_flow_id_restarts() {
        let mut core = NexusCore::default();
        let started = core.apply(Query::Start {
            flow_type: "codex-medium".into(),
            goal: "g".into(),
            origin: Origin {
                parent_flow_id: "owner".into(),
                session: "s".into(),
                turn: "t".into(),
            },
        });
        let Response::Started { flow_id, .. } = started else {
            panic!()
        };
        assert!(matches!(
            core.apply(Query::Restart {
                flow_id: flow_id.clone(),
                authority_flow_id: "other".into()
            }),
            Response::RestartRejected
        ));
        assert!(matches!(
            core.apply(Query::Restart {
                flow_id,
                authority_flow_id: "owner".into()
            }),
            Response::Restarted { generation: 2, .. }
        ));
    }
    #[test]
    fn socket_frames_typed_query() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("ordinary.sock");
        let serving = socket.clone();
        let worker = thread::spawn(move || {
            let mut core = NexusCore::default();
            core.serve_once(&serving).unwrap();
        });
        while !socket.exists() {
            thread::sleep(Duration::from_millis(2))
        }
        let mut peer = UnixStream::connect(&socket).unwrap();
        Frame::write_query(
            &mut peer,
            &Query::Start {
                flow_type: "codex-medium".into(),
                goal: "g".into(),
                origin: Origin {
                    parent_flow_id: "owner".into(),
                    session: "s".into(),
                    turn: "t".into(),
                },
            },
        )
        .unwrap();
        assert!(matches!(
            Frame::read_response(&mut peer).unwrap(),
            Response::Started { .. }
        ));
        worker.join().unwrap();
    }
}
