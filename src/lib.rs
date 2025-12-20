//! xrpc-rs: High-performance local IPC library for Rust.

pub mod channel;
pub mod client;
pub mod codec;
pub mod error;
pub mod message;
pub mod pool;
pub mod server;
pub mod streaming;
pub mod transport;

// Layer 1: FrameTransport
pub use transport::arc::{ArcFrameTransport, ZeroCopyTransport};
pub use transport::channel::{ChannelConfig, ChannelFrameTransport};
pub use transport::shared_memory::{RetryPolicy, SharedMemoryConfig, SharedMemoryFrameTransport};
pub use transport::tcp::{TcpConfig, TcpFrameTransport, TcpFrameTransportListener};
pub use transport::{FrameTransport, TransportStats, spawn_weak_loop};

#[cfg(unix)]
pub use transport::unix::{UnixConfig, UnixFrameTransport, UnixFrameTransportListener};

// Layer 2: Channel
pub use channel::message::{MessageChannel, MessageChannelAdapter};
pub use channel::serde::SerdeChannel;
pub use channel::typed::TypedChannel;

// Layer 3: RPC
pub use client::{RpcClient, RpcClientHandle};
pub use pool::{ConnectionPool, PoolConfig, PoolGuard};
pub use server::{
    FnHandler, FnStreamHandler, Handler, RpcServer, ServerHandle, ServerStreamSender,
    StreamHandler, TypedHandler, TypedStreamHandler,
};
pub use streaming::{StreamId, StreamManager, StreamReceiver, StreamSender, next_stream_id};

// Codec
#[cfg(feature = "codec-cbor")]
pub use codec::CborCodec;
#[cfg(feature = "codec-messagepack")]
pub use codec::MessagePackCodec;
#[cfg(feature = "codec-postcard")]
pub use codec::PostcardCodec;
pub use codec::{BincodeCodec, Codec, JsonCodec};

// Error types
pub use error::{Result, RpcError, TransportError, TransportResult};

// Message types
pub use message::Message;
pub use message::types::{MessageId, MessageType};

// Deprecated aliases for backward compatibility
#[deprecated(since = "0.2.0", note = "Use ArcFrameTransport instead")]
pub type ArcTransport = ArcFrameTransport;

#[deprecated(since = "0.2.0", note = "Use ChannelFrameTransport instead")]
pub type ChannelTransport = ChannelFrameTransport;

#[deprecated(since = "0.2.0", note = "Use SharedMemoryFrameTransport instead")]
pub type SharedMemoryTransport = SharedMemoryFrameTransport;

#[deprecated(since = "0.2.0", note = "Use TcpFrameTransport instead")]
pub type TcpTransport = TcpFrameTransport;

#[deprecated(since = "0.2.0", note = "Use TcpFrameTransportListener instead")]
pub type TcpTransportListener = TcpFrameTransportListener;

#[cfg(unix)]
#[deprecated(since = "0.2.0", note = "Use UnixFrameTransport instead")]
pub type UnixSocketTransport = UnixFrameTransport;

#[cfg(unix)]
#[deprecated(since = "0.2.0", note = "Use UnixFrameTransportListener instead")]
pub type UnixSocketListener = UnixFrameTransportListener;

#[deprecated(since = "0.2.0", note = "Use MessageChannelAdapter instead")]
pub type MessageTransportAdapter<F, C = BincodeCodec> = MessageChannelAdapter<F, C>;

#[deprecated(since = "0.2.0", note = "Use SerdeChannel instead")]
pub type RawTransport<F> = SerdeChannel<F, BincodeCodec>;
