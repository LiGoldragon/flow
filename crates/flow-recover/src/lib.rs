//! Recovery of which transcript is which permaflow's, read from the
//! transcripts themselves. The one input is a `Recovery` datom; every output
//! line is a `Placement` datom. What is read and how is the ethos's comment,
//! `ethos/recovery.ethos`.

pub mod generated;

pub use generated::recovery::*;

use datom_codec::{Actualizing, Budget, Composing, Datomizable, Potential};
use flow_ethos::{Address, FlowId};
use protos::{Compactable, Protosizable, ReaderBudget};
use serde_json::Value;
use signal_flow::TranscriptPath;
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
};

/// A value read from one datom text.
pub trait ReadsDatom: Sized {
    fn from_datom_text(text: &str) -> Result<Self, String>;
}

impl<T: Composing> ReadsDatom for T {
    fn from_datom_text(text: &str) -> Result<Self, String> {
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget::try_from(65_536).expect("a constant reader budget"),
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<T>::from(text)
            .actualize(&mut budget)
            .map_err(|error| format!("{error:?}"))
    }
}

/// A value printed as one datom text, the form flow-meta reads.
pub trait PrintsDatom {
    fn datom_text(&self) -> String;
}

impl<T: Datomizable> PrintsDatom for T {
    fn datom_text(&self) -> String {
        self.datomize(vec![]).protosize().compact()
    }
}

/// What one session file says of itself: the identity its launcher wrote,
/// if it names one, and the time of its last record that carries one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionReading {
    pub identity: Option<SessionIdentity>,
    pub time: Option<String>,
}

/// The identity lines of one session file, and the session id of the
/// record that held them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionIdentity {
    pub address: Address,
    pub flow_id: FlowId,
    pub session_id: Option<SessionId>,
}

/// One file that claims a flow id as an Address's, with its time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlowClaim {
    pub address: Address,
    pub time: Option<String>,
    pub file: PathBuf,
}

/// One flow in an Address's chain: its flow id, its newest time, and the
/// files that claim it; more than one file is a contested flow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainedFlow {
    pub flow_id: FlowId,
    pub time: Option<String>,
    pub files: Vec<PathBuf>,
}

/// The session files under the transcript root, no deeper than its Depth.
pub trait FindsSessionFiles {
    fn session_files(&self) -> Result<Vec<PathBuf>, String>;
}

/// The session files in one directory and, while depth remains, in the
/// directories below it.
pub trait CollectsSessionFiles {
    fn collect_session_files(&self, directory: &Path, depth: i64, files: &mut Vec<PathBuf>);
}

/// What one session file says of itself.
pub trait ReadsSession {
    fn session_reading(&self, file: &Path) -> SessionReading;
}

/// The Address and flow id the launcher's identity lines of one session's
/// prompts name: an Address line that reads as an Address and a Flow ID
/// line that is not empty.
pub trait ReadsPromptIdentity {
    fn prompt_identity(&self, prompts: &[&str]) -> Option<(Address, FlowId)>;
}

/// Whether a flow id is the start of the session id it was written in.
pub trait StartsSession {
    fn starts_session(&self, session_id: &SessionId) -> bool;
}

impl StartsSession for FlowId {
    fn starts_session(&self, session_id: &SessionId) -> bool {
        !self.0.is_empty() && session_id.0.starts_with(self.0.as_str())
    }
}

/// One Address's chain of flows, placed: its current flow bound, its older
/// flows past, its newest flows contested where they cannot be ordered.
pub trait PlacesChain {
    fn chain_placements(&self, address: &Address) -> Vec<Placement>;
}

impl PlacesChain for [ChainedFlow] {
    fn chain_placements(&self, address: &Address) -> Vec<Placement> {
        let mut flows: Vec<&ChainedFlow> = self.iter().collect();
        flows.sort_by(|one, other| {
            other
                .time
                .cmp(&one.time)
                .then_with(|| one.flow_id.cmp(&other.flow_id))
        });
        let Some(newest) = flows.first() else {
            return Vec::new();
        };
        let tied = flows
            .iter()
            .take_while(|flow| flow.time == newest.time)
            .count();
        let mut placements = Vec::new();
        if tied > 1 {
            placements.push(Placement::Contested(Contest::Unordered(Unordered_Data {
                address: address.clone(),
                transcript_path_vector: flows[..tied]
                    .iter()
                    .flat_map(|flow| flow.files.iter())
                    .map(|file| file.transcript_path())
                    .collect(),
            })));
        } else if let [file] = newest.files.as_slice() {
            placements.push(Placement::Bind(Bind_Data {
                address: address.clone(),
                transcript_path: file.transcript_path(),
            }));
        }
        for flow in &flows[tied..] {
            if let [file] = flow.files.as_slice() {
                placements.push(Placement::Past(Past_Data {
                    address: address.clone(),
                    flow_id: flow.flow_id.clone(),
                    transcript_path: file.transcript_path(),
                }));
            }
        }
        placements
    }
}

/// Every session file under the root, placed: the Binds, the Past flows,
/// the Contested flows and Addresses, the Mismatched files, then the
/// Unidentified ones.
pub trait PlacesSessions {
    fn placements(&self) -> Result<Vec<Placement>, String>;
}

/// A file's path as the meta Signal carries it.
pub trait AsTranscriptPath {
    fn transcript_path(&self) -> TranscriptPath;
}

impl AsTranscriptPath for Path {
    fn transcript_path(&self) -> TranscriptPath {
        TranscriptPath(self.to_string_lossy().into_owned())
    }
}

impl CollectsSessionFiles for FileSuffix {
    fn collect_session_files(&self, directory: &Path, depth: i64, files: &mut Vec<PathBuf>) {
        let Ok(entries) = fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if depth > 0 {
                    self.collect_session_files(&path, depth - 1, files);
                }
            } else if path.is_file()
                && path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().ends_with(self.0.as_str()))
            {
                files.push(path);
            }
        }
    }
}

impl FindsSessionFiles for Recovery {
    fn session_files(&self) -> Result<Vec<PathBuf>, String> {
        let root = Path::new(&self.transcript_root.0);
        fs::read_dir(root)
            .map_err(|error| format!("transcript root {}: {error}", root.display()))?;
        let mut files = Vec::new();
        self.session_shape
            .file_suffix
            .collect_session_files(root, self.depth.0, &mut files);
        files.sort();
        Ok(files)
    }
}

impl ReadsPromptIdentity for IdentityMarks {
    fn prompt_identity(&self, prompts: &[&str]) -> Option<(Address, FlowId)> {
        let mut address = None;
        let mut flow_id = None;
        for line in prompts.iter().flat_map(|prompt| prompt.lines()) {
            if let Some(rest) = line.strip_prefix(self.address_mark.0.as_str()) {
                address = address.or_else(|| Address::from_datom_text(rest.trim()).ok());
            }
            if let Some(rest) = line.strip_prefix(self.flow_id_mark.0.as_str()) {
                let rest = rest.trim();
                if !rest.is_empty() {
                    flow_id = flow_id.or_else(|| Some(FlowId(rest.to_owned())));
                }
            }
        }
        Some((address?, flow_id?))
    }
}

impl ReadsSession for Recovery {
    fn session_reading(&self, file: &Path) -> SessionReading {
        let shape = &self.session_shape;
        let kind = shape.snapshot_kind.0.as_str();
        let time_field = shape.time_field.0.as_str();
        let Ok(content) = fs::read(file) else {
            return SessionReading {
                identity: None,
                time: None,
            };
        };
        let records: Vec<&[u8]> = content.split(|byte| *byte == b'\n').collect();
        let mut identity = None;
        for line in &records {
            let text = String::from_utf8_lossy(line);
            if !text.contains(kind) {
                continue;
            }
            let Ok(record) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            let Some(snapshot) = record.get(&shape.record_field.0) else {
                continue;
            };
            if snapshot.get(&shape.kind_field.0).and_then(Value::as_str) != Some(kind) {
                continue;
            }
            let prompts: Vec<&str> = match snapshot.get(&shape.prompt_field.0) {
                Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).collect(),
                Some(Value::String(prompt)) => vec![prompt.as_str()],
                _ => continue,
            };
            if let Some((address, flow_id)) = self.identity_marks.prompt_identity(&prompts) {
                identity = Some(SessionIdentity {
                    address,
                    flow_id,
                    session_id: record
                        .get(&shape.session_field.0)
                        .and_then(Value::as_str)
                        .map(|session| SessionId(session.to_owned())),
                });
            }
        }
        let time = records.iter().rev().find_map(|line| {
            let text = String::from_utf8_lossy(line);
            if !text.contains(time_field) {
                return None;
            }
            serde_json::from_str::<Value>(&text)
                .ok()?
                .get(time_field)?
                .as_str()
                .map(str::to_owned)
        });
        SessionReading { identity, time }
    }
}

impl PlacesSessions for Recovery {
    fn placements(&self) -> Result<Vec<Placement>, String> {
        let mut flows: BTreeMap<FlowId, Vec<FlowClaim>> = BTreeMap::new();
        let mut mismatched = Vec::new();
        let mut unidentified = Vec::new();
        for file in self.session_files()? {
            let reading = self.session_reading(&file);
            let Some(identity) = reading.identity else {
                unidentified.push(Placement::Unidentified(file.transcript_path()));
                continue;
            };
            let started = identity
                .session_id
                .as_ref()
                .is_some_and(|session| identity.flow_id.starts_session(session));
            if !started {
                mismatched.push(Placement::Mismatched(Mismatched_Data {
                    transcript_path: file.transcript_path(),
                    flow_id: identity.flow_id,
                    session_id_option: identity.session_id,
                }));
                continue;
            }
            flows.entry(identity.flow_id).or_default().push(FlowClaim {
                address: identity.address,
                time: reading.time,
                file,
            });
        }
        let mut contested = Vec::new();
        let mut chains: HashMap<Address, Vec<ChainedFlow>> = HashMap::new();
        for (flow_id, claims) in flows {
            let files: Vec<PathBuf> = claims.iter().map(|claim| claim.file.clone()).collect();
            if files.len() > 1 {
                contested.push(Placement::Contested(Contest::SharedFlow(SharedFlow_Data {
                    flow_id: flow_id.clone(),
                    transcript_path_vector: files
                        .iter()
                        .map(|file| file.transcript_path())
                        .collect(),
                })));
            }
            let time = claims.iter().filter_map(|claim| claim.time.clone()).max();
            let mut addresses: Vec<&Address> = Vec::new();
            for claim in &claims {
                if !addresses.contains(&&claim.address) {
                    addresses.push(&claim.address);
                }
            }
            for address in addresses {
                chains
                    .entry(address.clone())
                    .or_default()
                    .push(ChainedFlow {
                        flow_id: flow_id.clone(),
                        time: time.clone(),
                        files: files.clone(),
                    });
            }
        }
        let mut binds = Vec::new();
        let mut past = Vec::new();
        for (address, chain) in chains {
            for placement in chain.chain_placements(&address) {
                match placement {
                    Placement::Bind(_) => binds.push(placement),
                    Placement::Past(_) => past.push(placement),
                    _ => contested.push(placement),
                }
            }
        }
        binds.sort_by_cached_key(PrintsDatom::datom_text);
        past.sort_by_cached_key(PrintsDatom::datom_text);
        contested.sort_by_cached_key(PrintsDatom::datom_text);
        binds.extend(past);
        binds.extend(contested);
        binds.extend(mismatched);
        binds.extend(unidentified);
        Ok(binds)
    }
}
