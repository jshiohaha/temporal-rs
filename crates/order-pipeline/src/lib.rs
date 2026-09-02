//! A realistic Temporal service: dependency injection, idempotent side effects,
//! retry policies tuned per failure mode, heartbeats, and saga compensation.
//!
//! Start with `hello-world` if you have not read that yet.

pub mod activities;
pub mod deps;
pub mod workflow;
