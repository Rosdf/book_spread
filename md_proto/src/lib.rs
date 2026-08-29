//! Wire types for the `md.v1` book feed.
//!
//! Messages only - no generated client or server. They travel as gRPC length-prefixed
//! messages; `md_wire::grpc` holds the framing constants and the refusal codes, and
//! `md_client` holds the generated tonic client.

pub mod md {
    pub mod v1 {
        #![allow(
            clippy::pedantic,
            clippy::nursery,
            clippy::restriction,
            missing_debug_implementations,
            reason = "prost-build generated code, not ours to lint"
        )]

        include!(concat!(env!("OUT_DIR"), "/md.v1.rs"));
    }
}
