//! Every connection greets first. A first frame that is not this
//! contract's greeting, on the ordinary socket or the meta one, is answered
//! the typed `GreetingRefused` and recorded here, metadata alone. A frame
//! after the greeting that decodes as no Input is refused in this
//! contract's words: `FrameRefused.UndecodableFrame`, naming the calling
//! process and its seat as Flow read them from the kernel and Herdr, and
//! the flow bound to that seat when there is one. Nothing in the frame is
//! read. An earlier client cannot read that answer, so the refusal is also
//! recorded, and `Observe.Undecodable` shows every record and each new one.

use crate::caller::{BindsCallerPane, CallerProcess, LocatesCallerPane};
use crate::sending::{GathersSeatEvidence, TypesSeatEvidence};
use crate::store::undecodable_frame::RecordsUndecodableFrames;
use crate::store::{AnnouncesLaunchChanges, ReadsFlowRows};
use crate::{HoldsStopRequest, RunningNexus};
use signal_flow::{Input, Output, Process, UndecodableFrame};
use std::sync::atomic::{AtomicBool, Ordering};

pub trait ReadsOrdinaryFrames {
    /// The Input `frame` carries. A frame that decodes as none is recorded
    /// and answered `FrameRefused` with what was read of its caller:
    /// `process`, its identity captured when the connection was accepted,
    /// and `caller`, the peer the kernel names, whose pane is its seat.
    fn ordinary_query(
        &self,
        frame: &[u8],
        process: Option<&Process>,
        caller: Option<CallerProcess>,
    ) -> Result<Input, Box<Output>>;
    /// Records a first frame that is not this contract's greeting, on the
    /// ordinary or the meta socket, after its typed refusal was written:
    /// metadata alone, as `ordinary_query` records a refused Input.
    fn refused_greeting(
        &self,
        process: Option<&Process>,
        caller: Option<CallerProcess>,
    ) -> UndecodableFrame;
}

/// The record a refused frame leaves, metadata alone: the calling process
/// and its seat as Flow read them from the kernel and Herdr, and the flow
/// bound to that seat when there is one. Nothing of the frame is kept.
trait ReadsRefusedCaller {
    fn refused_caller(
        &self,
        process: Option<&Process>,
        caller: Option<CallerProcess>,
    ) -> UndecodableFrame;
}

impl ReadsRefusedCaller for RunningNexus {
    fn refused_caller(
        &self,
        process: Option<&Process>,
        caller: Option<CallerProcess>,
    ) -> UndecodableFrame {
        let pane = caller.and_then(|caller| caller.caller_pane(&self.herdr));
        let flow = pane
            .as_ref()
            .and_then(|pane| self.bound_caller(pane).ok())
            .and_then(|bound| self.store.flow_node(&bound.flow_id.0).ok().flatten());
        let native_session = flow
            .as_ref()
            .map(|node| node.session_id.clone())
            .filter(|session| !session.0.is_empty());
        let process = process.cloned().unwrap_or(Process {
            pid: flow_ethos::generated::library::Pid(
                caller.map_or(flow_ethos::generated::library::Pid(0).0, |caller| {
                    i64::from(caller.process_id)
                }),
            ),
            started: flow_ethos::generated::library::Started(0),
        });
        UndecodableFrame {
            flow_id_option: flow.map(|node| node.flow_id),
            seat_evidence: self
                .seat_evidence(pane, native_session.map(|value| value.0))
                .typed(process),
        }
    }
}

impl ReadsOrdinaryFrames for RunningNexus {
    fn ordinary_query(
        &self,
        frame: &[u8],
        process: Option<&Process>,
        caller: Option<CallerProcess>,
    ) -> Result<Input, Box<Output>> {
        let decoded = rkyv::from_bytes::<Input, rkyv::rancor::Error>(frame);
        let error = match decoded {
            Ok(query) => return Ok(query),
            Err(error) => error,
        };
        let refused = self.refused_caller(process, caller);
        eprintln!(
            "flow-nexus: an ordinary frame decoded as no Input ({error}); refused: {refused:?}"
        );
        if let Err(store_error) = self.store.record_undecodable(refused.clone()) {
            eprintln!("flow-nexus: the undecodable frame was not recorded: {store_error}");
        }
        Err(Box::new(Output::FrameRefused(refused)))
    }

    fn refused_greeting(
        &self,
        process: Option<&Process>,
        caller: Option<CallerProcess>,
    ) -> UndecodableFrame {
        let refused = self.refused_caller(process, caller);
        eprintln!(
            "flow-nexus: a first frame was not this contract's greeting; refused: {refused:?}"
        );
        if let Err(store_error) = self.store.record_undecodable(refused.clone()) {
            eprintln!("flow-nexus: the refused greeting was not recorded: {store_error}");
        }
        refused
    }
}

pub trait ObservesUndecodableFrames {
    /// Sends every recorded undecodable frame, oldest first, in one
    /// `UndecodableObserved`, then one `UndecodableObserved` with each frame
    /// recorded later, until the observer goes or the Nexus stops. It waits
    /// on the store's announcements; nothing is re-read on a timer.
    fn observe_undecodable(
        &self,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Output) -> Result<(), String>,
    ) -> Result<(), String>;
}

impl ObservesUndecodableFrames for RunningNexus {
    fn observe_undecodable(
        &self,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Output) -> Result<(), String>,
    ) -> Result<(), String> {
        let changes = self.store.launch_changes.clone();
        let mut sent: Option<usize> = None;
        loop {
            let seen = changes.current();
            if disconnected.load(Ordering::Acquire) {
                return Err("the observing peer disconnected".into());
            }
            if self.stopping.requested() {
                return Err("the Nexus is stopping".into());
            }
            let frames = self
                .store
                .undecodable_frames()
                .map_err(|error| error.to_string())?;
            let from = sent.unwrap_or_default().min(frames.len());
            if sent.is_none() || frames.len() > from {
                send(&Output::UndecodableObserved(frames[from..].to_vec()))?;
                sent = Some(frames.len());
            }
            changes.after(seen);
        }
    }
}
