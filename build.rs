//! Compiles `tests/proto/echo.proto` into `OUT_DIR` for the tonic adapter's
//! integration tests (a real generated server/client over a real proto,
//! exercised in `tests/tonic.rs`).
//!
//! `generate_test_proto` is `#[cfg(feature = "tonic")]`, not just
//! runtime-skipped: `tonic-prost-build` is an optional build-dependency
//! activated only by that feature, so referencing it unconditionally would
//! fail to compile (not just fail to run) for a plain `cargo build`. This
//! keeps `protoc` and the codegen step required only on machines building or
//! testing the `tonic` feature.
fn main() {
    #[cfg(feature = "tonic")]
    generate_test_proto();
}

#[cfg(feature = "tonic")]
fn generate_test_proto() {
    println!("cargo:rerun-if-changed=tests/proto/echo.proto");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["tests/proto/echo.proto"], &["tests/proto"])
        .expect("failed to compile tests/proto/echo.proto");
}
