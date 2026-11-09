//! The client that speaks Signal to a Flow Nexus's ordinary socket: one
//! query out, its reply frames back. The `flow` CLI and `flow-hook` both
//! call through it, each with the socket it was given.

use crate::Invocation;
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use signal_flow::{ObserveSelection, Query, Response};
use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
};

pub struct FlowClient;

pub trait CallsFlowNexus {
    /// Sends one query and hands each reply frame on. Every query answers
    /// with one frame except Observe, whose frames run until the Nexus ends
    /// the exchange after the outcome.
    fn call(&self, invocation: &Invocation, each: &mut dyn FnMut(Response)) -> Result<(), String>;
}

pub trait TextualizesFlowReply {
    fn textualize_reply(&self, reply: &Response) -> String;
}

impl CallsFlowNexus for FlowClient {
    fn call(&self, invocation: &Invocation, each: &mut dyn FnMut(Response)) -> Result<(), String> {
        let query = &invocation.query;
        let mut peer = UnixStream::connect(&invocation.ordinary_socket_path)
            .map_err(|error| error.to_string())?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).map_err(|e| e.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        peer.write_all(&bytes).map_err(|e| e.to_string())?;
        let streams = matches!(
            query,
            Query::Observe(_)
                | Query::ObserveSends(_)
                | Query::ObserveSender(_)
                | Query::ObserveDeliveries(_)
        );
        let retirement_stream = matches!(query, Query::Observe(ObserveSelection::Retirement(_)));
        loop {
            let mut length = [0; 4];
            match peer.read_exact(&mut length) {
                Ok(()) => {}
                Err(error)
                    if streams
                        && !retirement_stream
                        && error.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    return Ok(());
                }
                Err(error)
                    if retirement_stream && error.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    return Err("retirement observation ended without a terminal failure; its evidence is incomplete".into());
                }
                Err(error) => return Err(error.to_string()),
            }
            let length = u32::from_be_bytes(length) as usize;
            if length > 1024 * 1024 {
                return Err("Signal frame exceeds 1 MiB".into());
            }
            let mut reply = vec![0; length];
            peer.read_exact(&mut reply).map_err(|e| e.to_string())?;
            let response = rkyv::from_bytes::<Response, rkyv::rancor::Error>(&reply)
                .map_err(|e| e.to_string())?;
            let retirement_failed = matches!(response, Response::RetirementObservationFailed(_));
            each(response);
            if retirement_failed {
                return Err("retirement observation failed; its evidence is incomplete".into());
            }
            if !streams {
                return Ok(());
            }
        }
    }
}

impl TextualizesFlowReply for FlowClient {
    fn textualize_reply(&self, reply: &Response) -> String {
        reply.datomize(vec![]).protosize().compact()
    }
}
