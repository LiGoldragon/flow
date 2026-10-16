//! Durable, requester-neutral admission for one disposable Field recovery.
//!
//! The admission boundary lives in `flow_nexus::recovery`; the installed
//! recovery service will invoke it from Lojix's daemon-owned job executor.

fn main() {
    eprintln!(
        "flow-recovery is an admission component; the Lojix daemon adapter is required before installation"
    );
    std::process::exit(2);
}
