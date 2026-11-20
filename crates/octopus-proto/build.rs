fn main() {
    println!("cargo:rerun-if-changed=proto/octopus.proto");
    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(&["proto/octopus.proto"], &["proto"])
        .expect("compile octopus.proto (needs protoc on PATH, or PROTOC)");
}
