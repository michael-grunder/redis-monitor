#![warn(clippy::all, clippy::nursery, clippy::pedantic)]
//! Reusable Redis/Valkey building blocks: `MONITOR` record parsing, command
//! metadata, and key extraction.
pub mod commands;
pub mod monitor;
