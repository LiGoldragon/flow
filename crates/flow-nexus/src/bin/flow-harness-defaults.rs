//! Renders Flow's complete production harness declarations after joining
//! deployment-owned executable paths, homes, sockets and model bindings.

use flow_nexus::RendersProductionHarnessConfiguration as _;
use std::process::ExitCode;

trait ReadsHarnessOverrides {
    fn harness_overrides(&self) -> Result<&str, String>;
}

impl ReadsHarnessOverrides for [String] {
    fn harness_overrides(&self) -> Result<&str, String> {
        let [source] = self else {
            return Err("usage: flow-harness-defaults '[ { claude <executable> <home> None [ ... ] } { codexStable <executable> <home> Some.<socket> [ ... ] } { codexNext <executable> <home> Some.<socket> [ ... ] } ]'".into());
        };
        Ok(source)
    }
}

fn main() -> ExitCode {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let rendered = arguments
        .harness_overrides()
        .and_then(str::render_production_harness_configuration);
    match rendered {
        Ok(query) => {
            println!("{query}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
