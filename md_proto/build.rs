//! Generates the `md.v1` message types.
//!
//! Plain prost structs and nothing else. `market_data.proto` does declare a service, but no
//! service generator is configured here, so prost-build ignores it: the server implements
//! `md.v1.MarketData` by hand over `h2` rather than through a generated codec, because a
//! codec would re-encode every book per client and the whole point of the fan-out is that it
//! does not. The generated *client* lives in `md_client`, which is free to use tonic - one
//! stream, no fan-out, nothing to lose.

const PROTO: &str = "proto/md/v1/market_data.proto";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.compile_protos(&[PROTO], &["proto"])?;

    println!("cargo:rerun-if-changed={PROTO}");
    Ok(())
}
