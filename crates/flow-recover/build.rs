use ethos_zero::{Actualizing, File, Generating, Potential};

fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    println!("cargo:rerun-if-changed=ethos/recovery.ethos");
    println!("cargo:rerun-if-changed=src/generated/recovery.rs");
    let source = std::fs::read_to_string(root.join("ethos/recovery.ethos")).expect("source");
    let file = Potential::<File>::from(source)
        .actualize()
        .expect("read Library");
    let generated = file.generate().expect("generate Library");
    assert_eq!(
        generated,
        std::fs::read_to_string(root.join("src/generated/recovery.rs")).expect("generated")
    );
}
