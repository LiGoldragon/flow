//! Ordinary Flow Signal contract. `ethos/signal.ethos` is its authored form.
use rkyv::{Archive, Deserialize, Serialize};
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    pub parent_flow_id: String,
    pub session: String,
    pub turn: String,
}
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum Query {
    Start {
        flow_type: String,
        goal: String,
        origin: Origin,
    },
    Restart {
        flow_id: String,
        authority_flow_id: String,
    },
}
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Started { flow_id: String, origin: Origin },
    Restarted { flow_id: String, generation: u64 },
    StartRejected,
    RestartRejected,
}
