use std::process::Command;

#[test]
fn composes_the_typed_meta_invocation() {
    let output = Command::new(env!("CARGO_BIN_EXE_flow-meta-compose-invocation"))
        .args(["/tmp/flow-meta.sock", "Configuration"])
        .output()
        .expect("run composer");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "{ /tmp/flow-meta.sock Configuration }\n"
    );
}

#[test]
fn refuses_an_invalid_query_before_any_client_transport() {
    let output = Command::new(env!("CARGO_BIN_EXE_flow-meta-compose-invocation"))
        .args(["/tmp/flow-meta.sock", "NoSuchQuery"])
        .output()
        .expect("run composer");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid meta Query"));
}
