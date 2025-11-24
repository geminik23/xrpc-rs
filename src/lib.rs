//! xRPC-rs - Local RPC library using shared memory
//!
//! Fast inter-process communication for Rust.
pub mod error;
pub mod protocol;
pub mod transport;

pub use error::{Result, RpcError, TransportError, TransportResult};
pub use protocol::{Message, MessageId, MessageType};
pub use transport::arc::ArcTransport;
pub use transport::channel::{ChannelConfig, ChannelTransport};
pub use transport::direct::{RawTransport, TypedChannel};
pub use transport::shared_memory::{RetryPolicy, SharedMemoryConfig, SharedMemoryTransport};
pub use transport::{Transport, TransportStats, spawn_weak_loop};
