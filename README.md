# xRPC-rs

[![Crates.io](https://img.shields.io/crates/v/xrpc-rs)](https://crates.io/crates/xrpc-rs)
[![Documentation](https://docs.rs/xrpc-rs/badge.svg)](https://docs.rs/xrpc-rs)
[![License](https://img.shields.io/crates/l/xrpc-rs)](LICENSE)

High-performance local IPC library for Rust with seamless transport flexibility.

Start with in-process channels for development, scale to shared memory for production IPC, extend to TCP for network deployment—same interface.

**Status:** Early prototype - working but not production-ready.

## Quick Start

```rust
use xrpc::{
    RpcClient, RpcServer, MessageChannelAdapter,
    SharedMemoryFrameTransport, SharedMemoryConfig,
};
use serde::{Serialize, Deserialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct AddRequest { a: i32, b: i32 }

#[derive(Serialize, Deserialize)]
struct AddResponse { result: i32 }

// =================== Server
let transport = SharedMemoryFrameTransport::create_server("my_service", SharedMemoryConfig::default())?;
let channel = Arc::new(MessageChannelAdapter::new(transport));

let server = RpcServer::new();
server.register_typed("add", |req: AddRequest| async move {
    Ok(AddResponse { result: req.a + req.b })
});
server.serve(channel).await?;

// =================== Client
let transport = SharedMemoryFrameTransport::connect_client("my_service")?;
let channel = MessageChannelAdapter::new(transport);

let client = RpcClient::new(channel);
let _handle = client.start();
let resp: AddResponse = client.call("add", &AddRequest { a: 1, b: 2 }).await?;
assert_eq!(resp.result, 3);
```

## Installation

```toml
[dependencies]
xrpc-rs = "0.2"

# Optional codecs: codec-messagepack, codec-cbor, codec-postcard, codec-all
xrpc-rs = { version = "0.2", features = ["codec-messagepack"] }
```

## Status

| Feature | Status |
|---------|--------|
| FrameTransport Layer (TCP, Unix, SharedMemory, Channel) | Completed |
| MessageChannel with compression | Completed |
| RPC Client/Server | Completed |
| Streaming | Completed |
| Connection Pooling | Completed |
| Batching | Planned |
| Service Discovery & Load Balancing | Planned |

## Architecture

xRPC follows a layered architecture:

| Layer | Trait/Module | Description |
|-------|--------------|-------------|
| Layer 1 | `FrameTransport` | Low-level byte transmission with framing |
| Layer 2 | `MessageChannel` | Message-aware channel with compression |
| Layer 3 | `RpcClient/RpcServer` | RPC with method dispatch, streaming |
| Layer 4 | Advanced | Batching, service discovery, load balancing |

```
Application
    ↓
RpcClient/RpcServer (Layer 3)
    ↓
MessageChannel (Layer 2)
    ↓
FrameTransport (Layer 1)
    ↓
Network/IPC
```

## Transport Comparison

| Transport | Use Case | Cross-Process | Serialization |
|-----------|----------|---------------|---------------|
| `SharedMemoryFrameTransport` | Production IPC | Yes | Yes |
| `TcpFrameTransport` | Network / Remote | Yes | Yes |
| `UnixFrameTransport` | Local IPC (Unix) | Yes | Yes |
| `ChannelFrameTransport` | Same-process / Testing | No | Yes |
| `ArcFrameTransport` | Same-process fast path | No | No (zero-copy) |

## Documentation

- [Message Protocol](./docs/message.md) - Binary message format specification

## Examples

### RPC Client/Server

```bash
# Terminal 1
cargo run --example rpc_client_server -- server

# Terminal 2
cargo run --example rpc_client_server -- client
```

### SharedMemory Transport

```bash
# Terminal 1
cargo run --example message_transport_shm -- server

# Terminal 2
cargo run --example message_transport_shm -- client
```

### Byte-level Transports

```bash
cargo run --example byte_transports
```

## License

MIT License - see [LICENSE](./LICENSE) for details.
