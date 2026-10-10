//! The Operation root is generated from `ethos/operation.ethos` by
//! ethos-zero; the committed module must equal a fresh generation, so a
//! stale `src/generated/operation.rs` fails the build.

use ethos_zero::{Actualizing, File, Generating, Potential};

fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    println!("cargo:rerun-if-changed=ethos/operation.ethos");
    println!("cargo:rerun-if-changed=src/generated/operation.rs");
    println!("cargo:rerun-if-changed=ethos/library.ethos");
    println!("cargo:rerun-if-changed=src/generated/library.rs");
    for (stem, root_name) in [("operation", "Operation"), ("library", "Library")] {
        let source =
            std::fs::read_to_string(root.join(format!("ethos/{stem}.ethos"))).expect("source");
        let file = Potential::<File>::from(source)
            .actualize()
            .unwrap_or_else(|_| panic!("read {root_name}"));
        let generated = file
            .generate()
            .unwrap_or_else(|_| panic!("generate {root_name}"));
        assert_eq!(
            generated,
            std::fs::read_to_string(root.join(format!("src/generated/{stem}.rs")))
                .expect("generated"),
            "src/generated/{stem}.rs is stale; regenerate it from ethos/{stem}.ethos"
        );
    }
}
