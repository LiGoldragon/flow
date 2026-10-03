//! The Operation root is generated from `ethos/operation.ethos` by
//! ethos-zero; the committed module must equal a fresh generation, so a
//! stale `src/generated/operation.rs` fails the build.

use ethos_zero::{Actualizing, File, Generating, Potential};

fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    println!("cargo:rerun-if-changed=ethos/operation.ethos");
    println!("cargo:rerun-if-changed=src/generated/operation.rs");
    let source = std::fs::read_to_string(root.join("ethos/operation.ethos")).expect("source");
    let file = Potential::<File>::from(source)
        .actualize()
        .unwrap_or_else(|_| panic!("read Operation"));
    let generated = file
        .generate()
        .unwrap_or_else(|_| panic!("generate Operation"));
    assert_eq!(
        generated,
        std::fs::read_to_string(root.join("src/generated/operation.rs")).expect("generated"),
        "src/generated/operation.rs is stale; regenerate it from ethos/operation.ethos"
    );
}
