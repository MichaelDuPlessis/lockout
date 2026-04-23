//! A collection of lock-free utilities for Rust.
//!
//! This crate is a workspace umbrella that re-exports:
//! - [`hazard`](https://docs.rs/lockout-hazard) — hazard pointers for safe memory reclamation
//! - [`channel`](https://docs.rs/lockout-channel) — lock-free MPMC channel

pub use lockout_channel as channel;
pub use lockout_hazard as hazard;
