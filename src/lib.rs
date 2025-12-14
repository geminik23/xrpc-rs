pub mod client;
pub mod codec;
pub mod error;
pub mod message;
pub mod pool;
pub mod server;
pub mod streaming;
pub mod transport;
pub mod typed_channel;

pub use client::{RpcClient, RpcClientHandle};
pub use codec::{BincodeCodec, Codec, JsonCodec};
pub use error::{Result, RpcError, TransportError, TransportResult};
pub use message::Message;
pub use message::types::{MessageId, MessageType};
pub use server::{
    FnHandler, FnStreamHandler, Handler, RpcServer, ServerHandle, ServerStreamSender,
    StreamHandler, TypedHandler, TypedStreamHandler,
};
pub use streaming::{StreamId, StreamManager, StreamReceiver, StreamSender, next_stream_id};
pub use transport::arc::ArcTransport;
pub use transport::channel::{ChannelConfig, ChannelTransport};
pub use transport::direct::RawTransport;
pub use transport::message_transport::{MessageTransport, MessageTransportAdapter};
pub use transport::shared_memory::{RetryPolicy, SharedMemoryConfig, SharedMemoryTransport};
pub use transport::tcp::{TcpConfig, TcpTransport, TcpTransportListener};
pub use transport::{Transport, TransportStats, spawn_weak_loop};
pub use typed_channel::TypedChannel;

#[cfg(unix)]
pub use transport::unix::{UnixConfig, UnixSocketListener, UnixSocketTransport};
