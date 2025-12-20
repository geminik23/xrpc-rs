# xRPC-rs

[![Crates.io](https://img.shields.io/crates/v/xrpc-rs)](https://crates.io/crates/xrpc-rs)
[![Documentation](https://docs.rs/xrpc-rs/badge.svg)](https://docs.rs/xrpc-rs)
[![License](https://img.shields.io/crates/l/xrpc-rs)](LICENSE)

High-performance local IPC library for Rust with seamless transport flexibility.

Start with in-process channels for development, scale to shared memory for production IPC, extend to TCP for network deployment—same interface.

**Status:** Early prototype - working but not production-ready.

## Quick Start

```rust
use xrpc::{RpcClient, RpcServer, MessageTransportAdapter, SharedMemoryTransport};

// Server
let server = RpcServer::new();
server.register_typed("add", |req: AddRequest| async move {
    Ok(AddResponse { result: req.a + req.b })
});
server.serve(transport).await?;

// Client  
let client = RpcClient::new(transport);
let _handle = client.start();
let resp: AddResponse = client.call("add", &AddRequest { a: 1, b: 2 }).await?;
```

## Installation

```toml
[dependencies]
xrpc-rs = "0.1"
```

## Status

| Feature | Status |
|---------|--------|
| Transport Layer (TCP, Unix, SharedMemory, Channel) | Completed |
| MessageTransport with compression | Completed |
| RPC Client/Server | Completed |
| Streaming | Completed |
| Connection Pooling | Completed |
| Batching | Planned |
| Service Discovery & Load Balancing | Planned |

## Architecture

xRPC follows a layered architecture:

| Layer | Trait/Module | Description |
|-------|--------------|-------------|
| Layer 1 | `Transport` | Low-level byte transmission |
| Layer 2 | `MessageTransport` | Message-aware transport with compression |
| Layer 3 | `RpcClient/RpcServer` | RPC with method dispatch, streaming |
| Layer 4 | Advanced | Batching, service discovery, load balancing |

```
Application
    ↓
RpcClient/RpcServer (Layer 3)
    ↓
MessageTransport (Layer 2)
    ↓
Transport (Layer 1)
    ↓
Network/IPC
```

## Transport Comparison

| Transport | Use Case | Cross-Process | Serialization |
|-----------|----------|---------------|---------------|
| `SharedMemoryTransport` | Production IPC | Yes | Yes |
| `TcpTransport` | Network / Remote | Yes | Yes |
| `UnixSocketTransport` | Local IPC (Unix) | Yes | Yes |
| `ChannelTransport` | Same-process / Testing | No | Yes |
| `ArcTransport` | Same-process fast path | No | No (zero-copy) |

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
