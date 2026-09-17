//! Flow Nexus dispatches typed ordinary and privileged Signal requests.
pub mod codex;
pub mod store;

use codex::{CodexAdapter, ConsumesResetCredit, ResumesCodex};
use signal_flow::{Query, Response, RestartRejection, StartRejection};
use std::{
    fs,
    io::{Read, Write},
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::Path,
};
use store::{
    AppliesFlowQuery, AuthorizesFlowRestart, ConfiguresFlowStore, ConfirmsStartedFlow, FlowStore,
    OpensFlowStore, RecordsPendingThread, RecordsRestartedFlow, RegistersFlowIdentity,
    ReservesPendingStart,
};

pub struct RunningNexus {
    pub store: FlowStore,
    pub codex: CodexAdapter,
}

pub trait Dispatches {
    fn dispatch(&self, query: Query) -> Response;
    fn dispatch_meta(&self, query: meta_signal_flow::Query) -> meta_signal_flow::Response;
}

impl Dispatches for RunningNexus {
    fn dispatch(&self, query: Query) -> Response {
        match query {
            Query::Start(request) => {
                let origin = request.origin_clue.clone();
                let goal = match request.flow_type.as_str() {
                    "codex-medium" => {
                        "Follow the predefined Codex medium flow procedure. Read the origin clue first, recover the caller's goal from its transcript, then carry the work to completion."
                    }
                    _ => return Response::StartRejected(StartRejection::UnknownFlowType),
                };
                let Ok(Some(pending)) = self.store.reserve_pending_start(Query::Start(request))
                else {
                    return Response::StartRejected(StartRejection::LaunchRefused);
                };
                let launched =
                    self.codex
                        .start_codex_observed(&pending.flow_id, goal, &origin, |thread| {
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
                        });
                match launched {
                    Ok(_) => self
                        .store
                        .confirm_started(&pending.flow_id)
                        .unwrap_or(Response::StartRejected(StartRejection::LaunchRefused)),
                    Err(_) => Response::StartRejected(StartRejection::LaunchRefused),
                }
            }
            Query::Restart(request) => {
                let authorization = self
                    .store
                    .authorize_restart(&request.flow_id, &request.origin_clue.flow_id);
                let Ok(Some(token)) = authorization else {
                    return Response::RestartRejected(RestartRejection::ProvenanceMismatch);
                };
                if request.origin_clue.session_id != token.thread_id {
                    return Response::RestartRejected(RestartRejection::ProvenanceMismatch);
                }
                let origin = signal_flow::OriginClue {
                    flow_id: token.authority_flow_id.clone(),
                    session_id: token.thread_id.clone(),
                    turn_id: "restart".into(),
                };
                if self
                    .codex
                    .resume_codex(&token.thread_id, "Resume this Flow.", &origin)
                    .is_err()
                {
                    return Response::RestartRejected(RestartRejection::ResumeRefused);
                }
                self.store
                    .record_restarted(token)
                    .unwrap_or(Response::RestartRejected(RestartRejection::ResumeRefused))
            }
            Query::ResolveRecipient(flow_id) => self
                .store
                .apply(Query::ResolveRecipient(flow_id))
                .unwrap_or(Response::RecipientResolutionRejected(
                    signal_flow::RecipientResolutionRejection::FlowUnavailable,
                )),
        }
    }

    fn dispatch_meta(&self, query: meta_signal_flow::Query) -> meta_signal_flow::Response {
        match query {
            meta_signal_flow::Query::Configure(configuration) => {
                if self.store.configure(configuration.clone()).is_err() {
                    return meta_signal_flow::Response::ConfigureRejected(
                        meta_signal_flow::ConfigureRejection::StoreRefused,
                    );
                }
                meta_signal_flow::Response::Configured(meta_signal_flow::Configured {
                    configuration,
                    activation: meta_signal_flow::Activation::NexusRestartRequired,
                })
            }
            meta_signal_flow::Query::ConsumeReset(request) => self
                .codex
                .consume_reset_credit(&request)
                .map(meta_signal_flow::Response::ResetConsumed)
                .unwrap_or(meta_signal_flow::Response::ResetRejected(
                    meta_signal_flow::ResetRejection::AdapterUnavailable,
                )),
            meta_signal_flow::Query::RegisterFlow(flow_node) => self
                .store
                .register_flow(flow_node)
                .map(meta_signal_flow::Response::FlowRegistered)
                .unwrap_or(meta_signal_flow::Response::FlowRegistrationRejected(
                    meta_signal_flow::FlowRegistrationRejection::StoreRefused,
                )),
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
        let listener = UnixListener::bind(socket).map_err(|error| error.to_string())?;
        fs::set_permissions(socket, fs::Permissions::from_mode(0o600))
            .map_err(|e| e.to_string())?;
        loop {
            let (mut peer, _) = listener.accept().map_err(|error| error.to_string())?;
            let query = Frame::read_query(&mut peer)?;
            let response = self.dispatch(query);
            Frame::write_response(&mut peer, &response)?;
        }
    }
}

pub trait ServesMeta {
    fn serve_meta(&self, socket: &Path) -> Result<(), String>;
}

impl ServesMeta for RunningNexus {
    fn serve_meta(&self, socket: &Path) -> Result<(), String> {
        let _ = fs::remove_file(socket);
        let listener = UnixListener::bind(socket).map_err(|error| error.to_string())?;
        fs::set_permissions(socket, fs::Permissions::from_mode(0o600))
            .map_err(|e| e.to_string())?;
        loop {
            let (mut peer, _) = listener.accept().map_err(|error| error.to_string())?;
            let reply = self.dispatch_meta(Frame::read_meta_query(&mut peer)?);
            Frame::write_meta_response(&mut peer, &reply)?;
        }
    }
}

pub struct Frame;

impl Frame {
    fn write_bytes(peer: &mut UnixStream, bytes: &[u8]) -> Result<(), String> {
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        peer.write_all(bytes).map_err(|e| e.to_string())
    }
    fn read_bytes(peer: &mut UnixStream) -> Result<Vec<u8>, String> {
        let mut length = [0; 4];
        peer.read_exact(&mut length).map_err(|e| e.to_string())?;
        let length = u32::from_be_bytes(length) as usize;
        if length > 1024 * 1024 {
            return Err("Signal frame exceeds 1 MiB".into());
        }
        let mut bytes = vec![0; length];
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
