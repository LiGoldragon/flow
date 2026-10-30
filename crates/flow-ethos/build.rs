use ethos_zero::{Actualizing, File, Generating, Potential};

fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    for name in ["library", "delivery", "message"] {
        let source_path = format!("ethos/{name}.ethos");
        let generated_path = format!("src/generated/{name}.rs");
        println!("cargo:rerun-if-changed={source_path}");
        println!("cargo:rerun-if-changed={generated_path}");
        let source = std::fs::read_to_string(root.join(source_path)).expect("source");
        let file = Potential::<File>::from(source)
            .actualize()
            .expect("read Library");
        let generated = file.generate().expect("generate Library");
        assert_eq!(
            generated,
            std::fs::read_to_string(root.join(generated_path)).expect("generated")
        );
    }
}
