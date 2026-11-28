//! Recovery of which transcript is which permaflow's, read from the
//! transcripts themselves. The one input is a `Recovery` datom; every output
//! line is a `Placement` datom. What is read and how is the ethos's comment,
//! `ethos/recovery.ethos`.

pub mod generated;

pub use generated::recovery::*;

use datom_codec::{Actualizing, Budget, Composing, Datomizable, Potential};
use flow_ethos::Address;
use protos::{Compactable, Protosizable, ReaderBudget};
use serde_json::Value;
use signal_flow::TranscriptPath;
use std::{
    collections::HashMap,
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    time::SystemTime,
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

/// The session files under the transcript root.
pub trait FindsSessionFiles {
    fn session_files(&self) -> Result<Vec<PathBuf>, String>;
}

/// The Address one session file names as its own, if it names one.
pub trait ReadsSessionIdentity {
    fn session_identity(&self, file: &Path) -> Option<Address>;
}

/// The Address the launcher's identity lines of one session's prompts name:
/// both an Address line that reads as an Address and a Flow ID line that
/// is not empty.
pub trait ReadsPromptIdentity {
    fn prompt_identity(&self, prompts: &[&str]) -> Option<Address>;
}

/// Every session file under the root, placed: one Bind for each Address,
/// then the Contested Addresses, then the Unidentified files.
pub trait PlacesSessions {
    fn placements(&self) -> Result<Vec<Placement>, String>;
}

/// When a file was last written; a file whose time cannot be read is the
/// oldest.
pub trait WrittenAt {
    fn written_at(&self) -> SystemTime;
}

impl WrittenAt for Path {
    fn written_at(&self) -> SystemTime {
        fs::metadata(self)
            .and_then(|metadata| metadata.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH)
    }
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

impl FindsSessionFiles for Recovery {
    fn session_files(&self) -> Result<Vec<PathBuf>, String> {
        let root = Path::new(&self.transcript_root.0);
        let suffix = self.session_shape.file_suffix.0.as_str();
        let projects = fs::read_dir(root)
            .map_err(|error| format!("transcript root {}: {error}", root.display()))?;
        let mut files = Vec::new();
        for project in projects.flatten() {
            let project = project.path();
            if !project.is_dir() {
                continue;
            }
            let Ok(entries) = fs::read_dir(&project) else {
                continue;
            };
            for entry in entries.flatten() {
                let file = entry.path();
                let named = file
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().ends_with(suffix));
                if named && file.is_file() {
                    files.push(file);
                }
            }
        }
        files.sort();
        Ok(files)
    }
}

impl ReadsPromptIdentity for IdentityMarks {
    fn prompt_identity(&self, prompts: &[&str]) -> Option<Address> {
        let mut address = None;
        let mut flow_id = None;
        for line in prompts.iter().flat_map(|prompt| prompt.lines()) {
            if let Some(rest) = line.strip_prefix(self.address_mark.0.as_str()) {
                address = address.or_else(|| Address::from_datom_text(rest.trim()).ok());
            }
            if let Some(rest) = line.strip_prefix(self.flow_id_mark.0.as_str()) {
                let rest = rest.trim();
                if !rest.is_empty() {
                    flow_id = flow_id.or(Some(rest));
                }
            }
        }
        flow_id.and(address)
    }
}

impl ReadsSessionIdentity for Recovery {
    fn session_identity(&self, file: &Path) -> Option<Address> {
        let shape = &self.session_shape;
        let kind = shape.snapshot_kind.0.as_str();
        let reader = BufReader::new(fs::File::open(file).ok()?);
        let mut held = None;
        for line in reader.split(b'\n') {
            let Ok(line) = line else {
                break;
            };
            let text = String::from_utf8_lossy(&line);
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
            if let Some(address) = self.identity_marks.prompt_identity(&prompts) {
                held = Some(address);
            }
        }
        held
    }
}

impl PlacesSessions for Recovery {
    fn placements(&self) -> Result<Vec<Placement>, String> {
        let mut claims: HashMap<Address, Vec<(SystemTime, PathBuf)>> = HashMap::new();
        let mut unidentified = Vec::new();
        for file in self.session_files()? {
            match self.session_identity(&file) {
                Some(address) => claims
                    .entry(address)
                    .or_default()
                    .push((file.written_at(), file)),
                None => unidentified.push(Placement::Unidentified(file.transcript_path())),
            }
        }
        let mut binds = Vec::new();
        let mut contested = Vec::new();
        for (address, mut files) in claims {
            files.sort_by(|one, other| other.0.cmp(&one.0).then_with(|| one.1.cmp(&other.1)));
            let others: Vec<TranscriptPath> = files
                .iter()
                .skip(1)
                .map(|(_, file)| file.transcript_path())
                .collect();
            binds.push(Placement::Bind(Bind_Data {
                address: address.clone(),
                transcript_path: files[0].1.transcript_path(),
            }));
            if !others.is_empty() {
                contested.push(Placement::Contested(Contested_Data {
                    address,
                    transcript_path_vector: others,
                }));
            }
        }
        binds.sort_by_cached_key(PrintsDatom::datom_text);
        contested.sort_by_cached_key(PrintsDatom::datom_text);
        binds.extend(contested);
        binds.extend(unidentified);
        Ok(binds)
    }
}
