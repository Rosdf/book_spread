# md_proto

Generated Rust types for the `md.v1` book feed - messages only, no generated client or server.
`build.rs` compiles `proto/md/v1/market_data.proto` with `prost-build` into `OUT_DIR`, and
`lib.rs` `include!`s the result under `md_proto::md::v1`.

The `.proto` does declare a service, but no service generator is configured, so prost-build
ignores it: `md_server` implements `md.v1.MarketData` by hand over `h2` rather than through a
generated codec, because a codec would re-encode every book per client - see that crate's
README for why the whole point is that it doesn't. `md_wire::grpc` holds the framing constants,
paths, and refusal codes both ends of that hand-rolled protocol agree on. `md_client` holds the
generated tonic client that drives an ordinary connection with these same message types - one
stream, no fan-out, nothing to lose by generating it; that crate is being reworked.

The generated module is exempted from this workspace's lints (`clippy::pedantic`,
`clippy::nursery`, `clippy::restriction`, `missing_debug_implementations`) - it isn't code
anyone here wrote, and holding it to house style would just mean fighting the generator.
