use flow_nexus::role_tool_policy::{PersonaRole, compile_file_and_record};
use serde_json::json;
use std::{env, path::Path};

fn main() {
    let arguments: Vec<_> = env::args().skip(1).collect();
    if arguments.len() != 4 || arguments[0] != "compile" {
        eprintln!("usage: flow-role-tool-manifest compile ROLE CATALOG.json RECEIPT.json");
        std::process::exit(2);
    }
    let role =
        PersonaRole::parse(&arguments[1]).unwrap_or_else(|refusal| refuse(refusal.to_string()));
    let compiled =
        compile_file_and_record(role, Path::new(&arguments[2]), Path::new(&arguments[3]))
            .unwrap_or_else(|refusal| refuse(refusal.to_string()));
    println!(
        "{}",
        serde_json::to_string(&json!({
            "receipt": compiled.receipt,
            "filtered_catalog": compiled.filtered_catalog,
            "claude_arguments": compiled.claude_arguments,
        }))
        .expect("output is JSON")
    );
}

fn refuse(message: String) -> ! {
    eprintln!("RoleToolManifestRefused: {message}");
    std::process::exit(1)
}
