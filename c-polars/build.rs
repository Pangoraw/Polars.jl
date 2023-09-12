pub fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();

    if true || std::env::var("GENERATE_INCLUDE").is_ok() {
        cbindgen::Builder::new()
            .with_crate(crate_dir)
            .with_pragma_once(true)
            .with_parse_expand(&["c-polars"])
            .with_include("arrow.h")
            .with_language(cbindgen::Language::C)
            .generate()
            .expect("could not build headers")
            .write_to_file("include/polars.h");
    }
}
