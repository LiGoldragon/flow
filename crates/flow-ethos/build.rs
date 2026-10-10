use ethos_zero::{Actualizing, File, Generating, Potential};

fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    println!("cargo:rerun-if-changed=ethos/library.ethos");
    println!("cargo:rerun-if-changed=src/generated/library.rs");
    let source = std::fs::read_to_string(root.join("ethos/library.ethos")).expect("source");
    let file = Potential::<File>::from(source)
        .actualize()
        .unwrap_or_else(|_| panic!("read Library"));
    let generated = file
        .generate()
        .unwrap_or_else(|_| panic!("generate Library"));
    assert_eq!(
        generated,
        std::fs::read_to_string(root.join("src/generated/library.rs")).expect("generated"),
        "src/generated/library.rs is stale; regenerate it from ethos/library.ethos"
    );
}
