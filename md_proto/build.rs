//! Generates the `md.v1` message types.
//!
//! Plain prost structs and nothing else. There is no service pass any more: the transport is
//! the length-prefixed framing in `md_wire::framing`, which carries these messages
//! directly, so nothing needs a generated client or server - and nothing needs the
//! `extern_path` substitution the gRPC response type used to require, because no codec is
//! re-encoding what the broadcaster already encoded.

const PROTO: &str = "proto/md/v1/market_data.proto";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.compile_protos(&[PROTO], &["proto"])?;

    println!("cargo:rerun-if-changed={PROTO}");
    Ok(())
}
