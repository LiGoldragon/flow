//! Whether this Flow opens the store at a path, before a deploy activates
//! it: `flow-store-check '{ /home/li/.local/state/flow-03521/flow.sema }'`
//! answers `Opens.<path>`, `Absent.<path>` or `Refused.{ <path> «reason» }`,
//! and exits non-zero only on a refusal.

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow_nexus::generated::startup::{ChecksStore, StoreCheck, StoreCheckRequest};
use protos::{Compactable, Protosizable, ReaderBudget};
use std::process::ExitCode;

trait ReadsStoreCheckRequest {
    fn store_check_request(&self) -> Result<StoreCheckRequest, String>;
}

impl ReadsStoreCheckRequest for [String] {
    fn store_check_request(&self) -> Result<StoreCheckRequest, String> {
        let [source] = self else {
            return Err("usage: flow-store-check '{ «store-path» }'".into());
        };
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<StoreCheckRequest>::from(source.clone())
            .actualize(&mut budget)
            .map_err(|error| format!("invalid store check datom: {error:?}"))
    }
}

fn main() -> ExitCode {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let request = match arguments.store_check_request() {
        Ok(request) => request,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let check = request.check();
    println!("{}", check.datomize(vec![]).protosize().compact());
    match check {
        StoreCheck::Opens(_) | StoreCheck::Absent(_) => ExitCode::SUCCESS,
        StoreCheck::Refused(_) => ExitCode::FAILURE,
    }
}
