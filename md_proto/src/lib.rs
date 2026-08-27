//! Wire types for the `md.v1` book feed.
//!
//! Messages only. They travel over the length-prefixed framing in `md_wire::framing`, which
//! is a plain TCP protocol rather than gRPC - see that module for how a connection is framed.

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
