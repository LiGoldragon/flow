//! Owner Flow Signal contract; configuration is only accepted on the meta socket.
use rkyv::{Archive, Deserialize, Serialize};
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Configuration {
    pub ordinary_socket: String,
    pub meta_socket: String,
}
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum Query {
    Configure(Configuration),
}
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Configured(Configuration),
}
