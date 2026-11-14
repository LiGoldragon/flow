//! The client that speaks Signal to a Flow Nexus's ordinary socket: one
//! query out, its reply frames back. The `flow` CLI and
//! `flow-nexus-hook` both call through it, each with the socket it was given.

use crate::Invocation;
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use signal_flow::{Input, ObserveSelection, Output};
use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
};

pub struct FlowClient;

pub trait CallsFlowNexus {
    /// Sends one query and hands each reply frame on. Every query answers
    /// with one frame except Observe, whose frames run until the Nexus ends
    /// the exchange after the outcome.
    fn call(&self, invocation: &Invocation, each: &mut dyn FnMut(Output)) -> Result<(), String>;
}

/// Every ordinary connection greets first: signal-flow's greeting is
/// written and the receipt read before any Input.
pub trait GreetsFlowNexus {
    /// Writes signal-flow's greeting and reads the receipt. Ok on Greeted
    /// with this contract's digest; any other receipt is the call's error
    /// and nothing more is written.
    fn greet_flow(&mut self) -> Result<(), String>;
}

impl GreetsFlowNexus for UnixStream {
    fn greet_flow(&mut self) -> Result<(), String> {
        use signal_flow::Contracted;
        let greeting =
            rkyv::to_bytes::<rkyv::rancor::Error>(&Input::greeting()).map_err(|e| e.to_string())?;
        self.write_all(&(greeting.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        self.write_all(&greeting).map_err(|e| e.to_string())?;
        let mut length = [0; 4];
        self.read_exact(&mut length).map_err(|e| e.to_string())?;
        let length = u32::from_be_bytes(length) as usize;
        if length > 1024 * 1024 {
            return Err("Signal frame exceeds 1 MiB".into());
        }
        let mut frame = vec![0; length];
        self.read_exact(&mut frame).map_err(|e| e.to_string())?;
        let receipt =
            rkyv::from_bytes::<signal_flow::HandshakeReceipt, rkyv::rancor::Error>(&frame)
                .map_err(|error| format!("ordinary greeting unreadable: {error}"))?;
        match receipt {
            signal_flow::HandshakeReceipt::Greeted(digest)
                if digest == Input::contract_digest() =>
            {
                Ok(())
            }
            receipt => Err(format!("ordinary greeting refused: {receipt:?}")),
        }
    }
}

pub trait TextualizesFlowReply {
    fn textualize_reply(&self, reply: &Output) -> String;
}

impl CallsFlowNexus for FlowClient {
    fn call(&self, invocation: &Invocation, each: &mut dyn FnMut(Output)) -> Result<(), String> {
        let query = &invocation.input;
        let mut peer = UnixStream::connect(&invocation.ordinary_socket_path.0)
            .map_err(|error| error.to_string())?;
        peer.greet_flow()?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).map_err(|e| e.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        peer.write_all(&bytes).map_err(|e| e.to_string())?;
        let streams = matches!(
            query,
            Input::Observe(_)
                | Input::ObserveSends(_)
                | Input::ObserveSender(_)
                | Input::ObserveDeliveries(_)
        );
        let retirement_stream = matches!(query, Input::Observe(ObserveSelection::Retirement(_)));
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
            let response = rkyv::from_bytes::<Output, rkyv::rancor::Error>(&reply)
                .map_err(|e| e.to_string())?;
            let retirement_failed = matches!(response, Output::RetirementObservationFailed(_));
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
    fn textualize_reply(&self, reply: &Output) -> String {
        reply.datomize(vec![]).protosize().compact()
    }
}
