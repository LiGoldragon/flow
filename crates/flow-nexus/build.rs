//! The Operation and Startup roots are generated from their Ethos files by
//! ethos-zero; committed modules must equal fresh generation.

use ethos_zero::{Actualizing, File, Generating, Potential};

fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    for name in [
        "operation",
        "startup",
        "library",
        "curriculum",
        "starter",
        "send_tool",
    ] {
        let source_path = format!("ethos/{name}.ethos");
        let generated_path = format!("src/generated/{name}.rs");
        println!("cargo:rerun-if-changed={source_path}");
        println!("cargo:rerun-if-changed={generated_path}");
        let source = std::fs::read_to_string(root.join(&source_path)).expect("source");
        let file = Potential::<File>::from(source)
            .actualize()
            .unwrap_or_else(|_| panic!("read {name}"));
        let generated = file
            .generate()
            .unwrap_or_else(|_| panic!("generate {name}"));
        assert_eq!(
            generated,
            std::fs::read_to_string(root.join(&generated_path)).expect("generated"),
            "{generated_path} is stale; regenerate it from {source_path}"
        );
    }
}
