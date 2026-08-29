//! Generates the `md.v1.MarketData` client.
//!
//! The client only: `build_server(false)`, because the server this talks to is hand-written
//! over `h2` - a generated one would re-encode every book per client, which is the cost the
//! fan-out exists to avoid. `extern_path` points the generated stubs at the message types
//! `md_proto` already generates, so nothing is defined twice and a `BookUpdate` from here is
//! the same type as one from there.

const PROTO: &str = "../md_proto/proto/md/v1/market_data.proto";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `tonic-prost-build` looks for `protoc` on the path rather than taking one; the vendored
    // binary is what keeps this building without a system install, exactly as `md_proto`'s own
    // build script does.
    //
    // SAFETY: `set_var` is unsound only against a concurrent read of the environment. This is
    // the first statement of a build script's `main`, nothing in this process has spawned a
    // thread yet, and nothing here will.
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .extern_path(".md.v1", "::md_proto::md::v1")
        .compile_protos(&[PROTO], &["../md_proto/proto"])?;

    println!("cargo:rerun-if-changed={PROTO}");
    Ok(())
}
