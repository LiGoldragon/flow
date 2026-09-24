//! Durable, requester-neutral admission for one disposable Field recovery.
//!
//! The installed recovery service will invoke this same admission boundary from
//! Lojix's daemon-owned job executor.  It deliberately has no incumbent Flow
//! identity: any surviving Flow supplies `requester_flow_id`, while the
//! immutable manifest and disposable target identify the work that dedupes.

use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    thread,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecoveryManifest {
    immutable_flake: String,
    disposable_flow_id: String,
    expected_title: String,
    expected_herdr_session: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecoveryRequest {
    requester_flow_id: String,
    manifest: RecoveryManifest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeEvidence {
    native_receipt: String,
    native_title: String,
    flow_id: String,
    herdr_session: String,
    herdr_agent: String,
    herdr_pane: String,
    herdr_terminal: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecoveryReceipt {
    generation: u64,
    native_receipt: String,
    native_title: String,
    flow_id: String,
    herdr_session: String,
    herdr_agent: String,
    herdr_pane: String,
    herdr_terminal: String,
}

trait LaunchesDisposableField: Send + Sync {
    fn launch(&self, manifest: &RecoveryManifest) -> Result<NativeEvidence, String>;
}

/// This observer is independent from the launcher. The installed adapter will
/// read native-thread, title, Flow registry, and Herdr route witnesses rather
/// than trusting fields returned by the deployment request.
trait ObservesReplacement: Send + Sync {
    fn observe(
        &self,
        manifest: &RecoveryManifest,
        launched: NativeEvidence,
    ) -> Result<RecoveryReceipt, String>;
}

struct DurableRecoveryExecutor<L, O> {
    state_directory: PathBuf,
    launcher: L,
    observer: O,
}

impl<L: LaunchesDisposableField, O: ObservesReplacement> DurableRecoveryExecutor<L, O> {
    fn request(&self, request: &RecoveryRequest) -> Result<RecoveryReceipt, String> {
        validate_manifest(&request.manifest)?;
        if request.requester_flow_id.is_empty() {
            return Err("recovery requester Flow ID is required".into());
        }
        fs::create_dir_all(&self.state_directory).map_err(io_error)?;
        let receipt = self.receipt_path(&request.manifest);
        let admission = self.admission_path(&request.manifest);
        loop {
            if receipt.exists() {
                return read_receipt(&receipt);
            }
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&admission)
            {
                Ok(mut lock) => {
                    lock.write_all(request.requester_flow_id.as_bytes())
                        .map_err(io_error)?;
                    lock.sync_all().map_err(io_error)?;
                    let result = self.execute(&request.manifest, &receipt);
                    let _ = fs::remove_file(&admission);
                    return result;
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => thread::yield_now(),
                Err(error) => return Err(io_error(error)),
            }
        }
    }

    fn execute(
        &self,
        manifest: &RecoveryManifest,
        receipt_path: &Path,
    ) -> Result<RecoveryReceipt, String> {
        let launched = self.launcher.launch(manifest)?;
        let receipt = self.observer.observe(manifest, launched)?;
        write_receipt(receipt_path, &receipt)?;
        Ok(receipt)
    }

    fn receipt_path(&self, manifest: &RecoveryManifest) -> PathBuf {
        self.state_directory
            .join(format!("{}.receipt", manifest_key(manifest)))
    }

    fn admission_path(&self, manifest: &RecoveryManifest) -> PathBuf {
        self.state_directory
            .join(format!("{}.admitting", manifest_key(manifest)))
    }
}

fn validate_manifest(manifest: &RecoveryManifest) -> Result<(), String> {
    let Some((prefix, revision)) = manifest.immutable_flake.rsplit_once('/') else {
        return Err("recovery manifest requires an immutable GitHub flake revision".into());
    };
    if !prefix.starts_with("github:")
        || revision.len() != 40
        || !revision
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err("recovery manifest requires github:<owner>/<repo>/<40 lowercase hex>".into());
    }
    if manifest.disposable_flow_id.is_empty()
        || manifest.expected_title.is_empty()
        || manifest.expected_herdr_session.is_empty()
    {
        return Err("recovery manifest omits disposable replacement identity".into());
    }
    Ok(())
}

fn manifest_key(manifest: &RecoveryManifest) -> String {
    let mut hash = Sha256::new();
    hash.update(manifest.immutable_flake.as_bytes());
    hash.update([0]);
    hash.update(manifest.disposable_flow_id.as_bytes());
    format!("{:x}", hash.finalize())
}

fn write_receipt(path: &Path, receipt: &RecoveryReceipt) -> Result<(), String> {
    let body = [
        receipt.generation.to_string(),
        receipt.native_receipt.clone(),
        receipt.native_title.clone(),
        receipt.flow_id.clone(),
        receipt.herdr_session.clone(),
        receipt.herdr_agent.clone(),
        receipt.herdr_pane.clone(),
        receipt.herdr_terminal.clone(),
    ]
    .join("\n");
    let temporary = path.with_extension("receipt.pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(io_error)?;
    file.write_all(body.as_bytes()).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    fs::rename(temporary, path).map_err(io_error)
}

fn read_receipt(path: &Path) -> Result<RecoveryReceipt, String> {
    let fields: Vec<_> = fs::read_to_string(path)
        .map_err(io_error)?
        .split('\n')
        .map(str::to_owned)
        .collect();
    if fields.len() != 8 {
        return Err("durable recovery receipt has an invalid shape".into());
    }
    Ok(RecoveryReceipt {
        generation: fields[0]
            .parse()
            .map_err(|_| "durable recovery receipt has an invalid generation")?,
        native_receipt: fields[1].clone(),
        native_title: fields[2].clone(),
        flow_id: fields[3].clone(),
        herdr_session: fields[4].clone(),
        herdr_agent: fields[5].clone(),
        herdr_pane: fields[6].clone(),
        herdr_terminal: fields[7].clone(),
    })
}

fn io_error(error: io::Error) -> String {
    error.to_string()
}

fn main() {
    eprintln!(
        "flow-recovery is an admission component; the Lojix daemon adapter is required before installation"
    );
    std::process::exit(2);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::Arc,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[derive(Clone)]
    struct Launcher(Arc<AtomicUsize>);

    impl LaunchesDisposableField for Launcher {
        fn launch(&self, manifest: &RecoveryManifest) -> Result<NativeEvidence, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(NativeEvidence {
                native_receipt: "thread-verified-receipt".into(),
                native_title: manifest.expected_title.clone(),
                flow_id: manifest.disposable_flow_id.clone(),
                herdr_session: manifest.expected_herdr_session.clone(),
                herdr_agent: "field-luna".into(),
                herdr_pane: "wF:pL".into(),
                herdr_terminal: "term-field-luna".into(),
            })
        }
    }

    struct WrongTitleLauncher;

    impl LaunchesDisposableField for WrongTitleLauncher {
        fn launch(&self, manifest: &RecoveryManifest) -> Result<NativeEvidence, String> {
            Ok(NativeEvidence {
                native_receipt: "thread-verified-receipt".into(),
                native_title: "Field Luna imposter".into(),
                flow_id: manifest.disposable_flow_id.clone(),
                herdr_session: manifest.expected_herdr_session.clone(),
                herdr_agent: "field-luna".into(),
                herdr_pane: "wF:pL".into(),
                herdr_terminal: "term-field-luna".into(),
            })
        }
    }

    struct IndependentObserver;

    impl ObservesReplacement for IndependentObserver {
        fn observe(
            &self,
            manifest: &RecoveryManifest,
            launched: NativeEvidence,
        ) -> Result<RecoveryReceipt, String> {
            if launched.native_receipt.is_empty()
                || launched.native_title != manifest.expected_title
                || launched.flow_id != manifest.disposable_flow_id
                || launched.herdr_session != manifest.expected_herdr_session
                || launched.herdr_agent.is_empty()
                || launched.herdr_pane.is_empty()
                || launched.herdr_terminal.is_empty()
            {
                return Err("independent replacement evidence did not match the manifest".into());
            }
            Ok(RecoveryReceipt {
                generation: 1,
                native_receipt: launched.native_receipt,
                native_title: launched.native_title,
                flow_id: launched.flow_id,
                herdr_session: launched.herdr_session,
                herdr_agent: launched.herdr_agent,
                herdr_pane: launched.herdr_pane,
                herdr_terminal: launched.herdr_terminal,
            })
        }
    }

    fn request(requester: &str) -> RecoveryRequest {
        RecoveryRequest {
            requester_flow_id: requester.into(),
            manifest: RecoveryManifest {
                immutable_flake:
                    "github:LiGoldragon/CriomOS/0123456789abcdef0123456789abcdef01234567".into(),
                disposable_flow_id: "field-luna-disposable".into(),
                expected_title: "Field Luna disposable".into(),
                expected_herdr_session: "field-recovery".into(),
            },
        }
    }

    #[test]
    fn any_two_surviving_flows_share_one_durable_execution_and_generation() {
        let directory = tempfile::tempdir().unwrap();
        let launches = Arc::new(AtomicUsize::new(0));
        let executor = Arc::new(DurableRecoveryExecutor {
            state_directory: directory.path().to_owned(),
            launcher: Launcher(launches.clone()),
            observer: IndependentObserver,
        });
        let first = {
            let executor = executor.clone();
            thread::spawn(move || executor.request(&request("survivor-a")).unwrap())
        };
        let second = {
            let executor = executor.clone();
            thread::spawn(move || executor.request(&request("survivor-b")).unwrap())
        };
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        assert_eq!(first, second);
        assert_eq!(first.generation, 1);
        assert_eq!(first.native_title, "Field Luna disposable");
        assert_eq!(first.flow_id, "field-luna-disposable");
        assert_eq!(first.herdr_pane, "wF:pL");

        let reopened = DurableRecoveryExecutor {
            state_directory: directory.path().to_owned(),
            launcher: Launcher(launches.clone()),
            observer: IndependentObserver,
        };
        assert_eq!(reopened.request(&request("successor-c")).unwrap(), first);
        assert_eq!(launches.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn mutable_or_incomplete_manifests_cannot_admit_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let executor = DurableRecoveryExecutor {
            state_directory: directory.path().to_owned(),
            launcher: Launcher(Arc::new(AtomicUsize::new(0))),
            observer: IndependentObserver,
        };
        let mut mutable = request("survivor-a");
        mutable.manifest.immutable_flake = "github:LiGoldragon/CriomOS/main".into();
        assert!(executor.request(&mutable).is_err());
    }

    #[test]
    fn mismatched_native_evidence_never_becomes_a_durable_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let executor = DurableRecoveryExecutor {
            state_directory: directory.path().to_owned(),
            launcher: WrongTitleLauncher,
            observer: IndependentObserver,
        };
        let request = request("survivor-a");
        assert!(executor.request(&request).is_err());
        assert!(!executor.receipt_path(&request.manifest).exists());
    }
}
