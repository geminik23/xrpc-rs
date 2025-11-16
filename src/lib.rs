//! xRPC-rs - Local RPC library using shared memory
//!
//! Fast inter-process communication for Rust.
pub mod error;
pub mod protocol;

pub use error::{Result, RpcError, TransportError, TransportResult};
pub use protocol::{Message, MessageId, MessageType};
