use signal_flow::{Origin, Query, Response};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
};
struct Datom;
impl Datom {
    fn query(text: &str) -> Result<Query, String> {
        let words = text
            .replace(['{', '}', '.', '«', '»'], " ")
            .split_whitespace()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        match words.first().map(String::as_str) { Some("Start") if words.len()==6=>Ok(Query::Start{flow_type:words[1].clone(),goal:words[2].clone(),origin:Origin{parent_flow_id:words[3].clone(),session:words[4].clone(),turn:words[5].clone()}}), Some("Restart") if words.len()==3=>Ok(Query::Restart{flow_id:words[1].clone(),authority_flow_id:words[2].clone()}), _=>Err("expected Start.{ type goal parent-flow session turn } or Restart.{ flow-id authority-flow-id }".into()) }
    }
}
struct Client;
impl Client {
    fn call(socket: &str, query: &Query) -> Result<Response, String> {
        let mut peer = UnixStream::connect(socket).map_err(|e| e.to_string())?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).map_err(|e| e.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        peer.write_all(&bytes).map_err(|e| e.to_string())?;
        let mut length = [0; 4];
        peer.read_exact(&mut length).map_err(|e| e.to_string())?;
        let mut reply = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut reply).map_err(|e| e.to_string())?;
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&reply).map_err(|e| e.to_string())
    }
}
fn main() {
    let mut arguments = env::args().skip(1);
    let Some(datom) = arguments.next() else {
        std::process::exit(2)
    };
    if arguments.next().is_some() {
        std::process::exit(2)
    };
    let socket = env::var("FLOW_SOCKET").unwrap_or_else(|_| "/tmp/flow-nexus.sock".into());
    match Datom::query(&datom).and_then(|query| Client::call(&socket, &query)) {
        Ok(reply) => println!("{reply:?}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2)
        }
    }
}
