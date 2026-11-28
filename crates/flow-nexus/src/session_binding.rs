//! Meta `Bind.{ Address TranscriptPath }`: the owner names a running
//! Claude session by its transcript, and the Address is Awake as its flow.
//! The session id is read from the transcript's first rows (each row's
//! `sessionId`, equal to the file's stem); the flow id is its first six
//! characters, the name a Harness Nexus gives it. The seat row is seeded
//! from that Harness Nexus's witness of the session, so a bound idle
//! session is Resting at once. Bind is an owner's override: its audit
//! transition is caused Overridden.
use crate::RunningNexus;
use crate::generated::memory::{SeatActivity, SeatStanding};
use crate::harness_subscription::KeepsHarnessWitnesses;
use crate::store::session_binding::{BindsNativeSessions, SessionBinding, SessionBindingRefusal};
use meta_signal_flow::{Bind_Data, BindRefused_Data, Bound_Data, Output};
use signal_flow::{FlowId, HarnessKind, HerdrRoute};
use std::io::{BufRead, BufReader};
use std::path::Path;

/// A Claude transcript's own session.
pub trait ReadsTranscriptSession {
    /// How many rows are read for it.
    const ROWS: usize = 64;
    /// The `sessionId` the first rows name, when it is the file's stem;
    /// None when the file cannot be read or names no such session.
    fn transcript_session(&self) -> Option<String>;
}

impl ReadsTranscriptSession for Path {
    fn transcript_session(&self) -> Option<String> {
        let stem = self.file_stem()?.to_str()?.to_owned();
        let file = std::fs::File::open(self).ok()?;
        BufReader::new(file)
            .lines()
            .take(Self::ROWS)
            .map_while(Result::ok)
            .find_map(|line| {
                serde_json::from_str::<serde_json::Value>(&line)
                    .ok()?
                    .get("sessionId")?
                    .as_str()
                    .map(str::to_owned)
            })
            .filter(|session| *session == stem)
    }
}

pub trait BindsSessions {
    /// Meta `Bind`: UnknownPermaflow when no permaflow is configured at
    /// the Address, Unreadable when no session is read from the
    /// transcript; else Bound, naming the flow the Address was Awake as
    /// before when it was another.
    fn bind_session(&self, bind: &Bind_Data) -> Result<Bound_Data, Output>;
    /// The route of the Herdr pane whose Claude agent shows `session`.
    fn route_showing_session(&self, session: &str) -> Option<HerdrRoute>;
    /// The seat standing a Harness Nexus tells of the session it names
    /// `told`, if one does.
    fn witnessed_standing(&self, told: &FlowId) -> Option<SeatStanding>;
}

impl BindsSessions for RunningNexus {
    fn bind_session(&self, bind: &Bind_Data) -> Result<Bound_Data, Output> {
        use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
        let store_refused = |error: String| {
            Output::Refused(meta_signal_flow::Refused_Data::StoreRefused(
                meta_signal_flow::StoreFailure(error),
            ))
        };
        match self.store.metaflow_declaration(&bind.address) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Err(Output::BindRefused(BindRefused_Data::UnknownPermaflow(
                    bind.address.clone(),
                )));
            }
            Err(error) => return Err(store_refused(error.to_string())),
        }
        let Some(session) = Path::new(&bind.transcript_path.0).transcript_session() else {
            return Err(Output::BindRefused(BindRefused_Data::Unreadable(
                bind.transcript_path.clone(),
            )));
        };
        let flow_id = FlowId(session.chars().take(6).collect());
        let route = self.route_showing_session(&session);
        let standing = self.witnessed_standing(&flow_id);
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.store.bind_native_session(SessionBinding {
            address: bind.address.clone(),
            flow_id: flow_id.clone(),
            session,
            route,
            standing,
        }) {
            Ok(Ok(replaced)) => {
                eprintln!(
                    "flow-nexus: {:?} bound to flow {} by its transcript {}",
                    bind.address, flow_id.0, bind.transcript_path.0
                );
                Ok(Bound_Data {
                    address: bind.address.clone(),
                    flow_id,
                    flow_id_option: replaced,
                })
            }
            Ok(Err(SessionBindingRefusal::OtherSession)) => Err(store_refused(format!(
                "flow {} is bound to another native session",
                flow_id.0
            ))),
            Err(error) => Err(store_refused(error.to_string())),
        }
    }

    fn route_showing_session(&self, session: &str) -> Option<HerdrRoute> {
        use crate::caller::KnowsHerdrServers;
        use crate::herdr::{PaneRoute, ReadsHerdrPanes};
        for listed in self.herdr.herdr_sessions()? {
            let Some(snapshot) = self.herdr.session_snapshot(&listed.name) else {
                continue;
            };
            let pane = snapshot
                .pointer("/result/snapshot/agents")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .find(|agent| {
                    agent
                        .pointer("/agent_session/value")
                        .and_then(serde_json::Value::as_str)
                        == Some(session)
                })
                .and_then(|agent| agent.get("pane_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            if let Some(pane) = pane
                && let PaneRoute::Shown(route) =
                    self.herdr
                        .pane_route(&listed.name, &pane, &HarnessKind::Claude)
            {
                return Some(route);
            }
        }
        None
    }

    fn witnessed_standing(&self, told: &FlowId) -> Option<SeatStanding> {
        use harness::{Flow, State};
        self.harness_witnesses
            .witnesses()
            .into_iter()
            .find(|(_, flow_id, _)| flow_id == told)
            .and_then(|(_, _, witnessed)| match witnessed.state {
                State::Flowing(Flow::Rested) => Some(SeatStanding::Alive(SeatActivity::Resting)),
                State::Flowing(_) => Some(SeatStanding::Alive(SeatActivity::Working)),
                State::Ended => Some(SeatStanding::Ended),
                State::Dry => None,
            })
    }
}
