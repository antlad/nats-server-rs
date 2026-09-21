//! Test-support code for the raw-protocol integration tests.
//!
//! `tests/wire.rs` (Part 1) keeps its own private helpers so none of its
//! assertions move; everything added since then shares [`wire`] instead of
//! re-implementing socket reads five times.
pub mod wire;
