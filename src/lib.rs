//! xRPC-rs - Local RPC library using shared memory
//!
//! Fast inter-process communication for Rust.
pub mod codec;
pub mod error;
pub mod message;
pub mod transport;
pub mod typed_channel;

pub use error::{Result, RpcError, TransportError, TransportResult};
pub use message::Message;
pub use message::types::{MessageId, MessageType};
pub use transport::arc::ArcTransport;
pub use transport::channel::{ChannelConfig, ChannelTransport};
pub use transport::direct::RawTransport;
pub use transport::shared_memory::{RetryPolicy, SharedMemoryConfig, SharedMemoryTransport};
pub use transport::{Transport, TransportStats, spawn_weak_loop};
pub use typed_channel::TypedChannel;
