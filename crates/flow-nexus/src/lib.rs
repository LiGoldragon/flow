//! Flow Nexus owns dispatch and identity. Its ordinary and meta transports
//! carry rkyv archives; JSON below is only the external Codex app-server RPC.
use signal_flow::{Query, Response};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
};
pub trait Applies {
    fn apply(&mut self, query: Query) -> Response;
}
pub trait StartsCodex {
    fn start_codex(&self, goal: &str) -> Result<String, String>;
}
#[derive(Default)]
pub struct NexusCore {
    flows: HashMap<String, (String, u64)>,
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
pub struct CodexAdapter {
    pub socket: String,
    pub model: String,
}
impl StartsCodex for CodexAdapter {
    fn start_codex(&self, goal: &str) -> Result<String, String> {
        let mut child = Command::new("codex")
            .args(["app-server", "proxy", "--sock", &self.socket])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| e.to_string())?;
        let mut output = BufReader::new(child.stdout.take().ok_or("proxy stdout unavailable")?);
        let input = child.stdin.as_mut().ok_or("proxy stdin unavailable")?;
        writeln!(input,"{{\"id\":1,\"method\":\"initialize\",\"params\":{{\"clientInfo\":{{\"name\":\"flow-nexus\",\"version\":\"0.1.0\"}}}}}}") .map_err(|e|e.to_string())?;
        let mut line = String::new();
        output.read_line(&mut line).map_err(|e| e.to_string())?;
        writeln!(input,"{{\"method\":\"initialized\"}}\n{{\"id\":2,\"method\":\"thread/start\",\"params\":{{\"cwd\":\"/home/li/primary\",\"model\":{:?},\"modelProvider\":\"openai\",\"sandbox\":\"danger-full-access\",\"approvalPolicy\":\"never\",\"ephemeral\":false,\"threadSource\":\"flow-nexus\"}}}}",self.model).map_err(|e|e.to_string())?;
        line.clear();
        output.read_line(&mut line).map_err(|e| e.to_string())?;
        let thread =
            serde_json::from_str::<serde_json::Value>(&line).map_err(|e| e.to_string())?["result"]
                ["thread"]["id"]
                .as_str()
                .ok_or("thread/start returned no thread id")?
                .to_owned();
        writeln!(input,"{{\"id\":3,\"method\":\"turn/start\",\"params\":{{\"threadId\":{:?},\"input\":[{{\"type\":\"text\",\"text\":{:?}}}],\"model\":{:?},\"effort\":\"medium\",\"turnTrigger\":\"flow-nexus\"}}}}",thread,goal,self.model).map_err(|e|e.to_string())?;
        Ok(thread)
    }
}
