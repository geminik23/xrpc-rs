# xRPC-rs

[![Crates.io](https://img.shields.io/crates/v/xrpc-rs)](https://crates.io/crates/xrpc-rs)
[![Documentation](https://docs.rs/xrpc-rs/badge.svg)](https://docs.rs/xrpc-rs)
[![License](https://img.shields.io/crates/l/xrpc-rs)](LICENSE)

Async RPC and local IPC library for Rust with transport flexibility.

Start with in-process channels, move to shared memory or Unix sockets for local IPC, and use TCP for network deployment—with the same layered interface.

**Status:** Actively developed. Core features are available, and the API may change between releases.

## Quick Start

This single-process example uses an in-process channel pair. The same RPC layer can run over shared memory, Unix sockets, or TCP by replacing the frame transport.

```rust
use serde::{Deserialize, Serialize};
use xrpc::{
    ChannelConfig, ChannelFrameTransport, MessageChannelAdapter, RpcClient, RpcServer,
};

#[derive(Serialize, Deserialize)]
struct AddRequest { a: i32, b: i32 }

#[derive(Serialize, Deserialize)]
struct AddResponse { result: i32 }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client_transport, server_transport) =
        ChannelFrameTransport::create_pair("quick-start", ChannelConfig::default())?;

    let server = RpcServer::new();
    server.register_typed("add", |req: AddRequest| async move {
        Ok(AddResponse { result: req.a + req.b })
    });
    let server_handle =
        server.spawn_handler(MessageChannelAdapter::new(server_transport));

    let client = RpcClient::new(MessageChannelAdapter::new(client_transport));
    // Retain the handle when explicit task joining or forced shutdown is needed.
    let client_handle = client.try_start()?;

    let response: AddResponse = client
        .call("add", &AddRequest { a: 1, b: 2 })
        .await?;
    assert_eq!(response.result, 3);

    let close_result = client.close().await;
    let join_result = client_handle.join().await;
    close_result?;
    join_result?;
    server_handle.shutdown().await;

    Ok(())
}
```

Calling `try_start()` is required before making RPCs. Retaining its handle is optional for ordinary calls, but it enables explicit receive-task joining and forced shutdown. This example retains it for strict cleanup confirmation.

## Installation

```toml
[dependencies]
xrpc-rs = "0.3.0"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

To enable an optional codec, replace the `xrpc-rs` dependency above with, for example:

```toml
xrpc-rs = { version = "0.3.0", features = ["codec-messagepack"] }
```

Available codec features are `codec-messagepack`, `codec-cbor`, `codec-postcard`, and `codec-all`. Bincode and JSON support are available without optional features.

xrpc-rs 0.3 requires Rust 1.85 or newer.

## Status

| Feature | Status |
|---------|--------|
| FrameTransport Layer (TCP, Unix, SharedMemory, Channel, Arc) | ✅ Completed |
| MessageChannel with compression | ✅ Completed |
| RPC Client/Server | ✅ Completed |
| Streaming | ✅ Completed |
| Connection Pooling | ✅ Completed |
| Service Discovery & Load Balancing | ✅ Completed |
| Docs & Examples | In Progress |
| Python Bindings | Under Consideration |

## Architecture

xRPC follows a layered architecture:

| Layer | Trait/Module | Description |
|-------|--------------|-------------|
| Layer 1 | `FrameTransport` | Low-level byte transmission with framing |
| Layer 2 | `MessageChannel` | Message-aware channel with compression |
| Layer 3 | `RpcClient/RpcServer` | RPC with method dispatch, streaming |
| Layer 4 | `LoadBalancer` | Service discovery, load balancing |

```
Application
    ↓
LoadBalancedClient (Layer 4)
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
| `SharedMemoryFrameTransport` | Cross-process local IPC, one client per named server | Yes | Yes |
| `TcpFrameTransport` | Network / Remote | Yes | Yes |
| `UnixFrameTransport` | Local IPC (Unix) | Yes | Yes |
| `ChannelFrameTransport` | Same-process / Testing | No | Yes |
| `ArcFrameTransport` | Same-process fast path | No | Frame/RPC: Yes; `ZeroCopyTransport`: No |

## Load Balancing

Distribute requests across multiple server instances:

```rust
use xrpc::{
    LoadBalancedClient, LoadBalancer, ClientFactory, Endpoint,
    StaticDiscovery, RoundRobin, RpcClient, MessageChannelAdapter,
    TcpFrameTransport, TcpConfig, BincodeCodec, RpcError,
};
use async_trait::async_trait;
use std::sync::Arc;

struct TcpFactory;

#[async_trait]
impl ClientFactory<MessageChannelAdapter<TcpFrameTransport>, BincodeCodec> for TcpFactory {
    async fn create(&self, endpoint: &Endpoint) 
        -> Result<RpcClient<MessageChannelAdapter<TcpFrameTransport>, BincodeCodec>, RpcError> 
    {
        let Endpoint::Tcp(addr) = endpoint else {
            return Err(RpcError::ClientError("Expected TCP".into()));
        };
        let transport = TcpFrameTransport::connect(*addr, TcpConfig::default())
            .await.map_err(RpcError::Transport)?;
        Ok(RpcClient::new(MessageChannelAdapter::new(transport)))
    }
}

// Setup load balancer
let endpoints = vec![
    Endpoint::tcp_from_str("127.0.0.1:8001")?,
    Endpoint::tcp_from_str("127.0.0.1:8002")?,
    Endpoint::tcp_from_str("127.0.0.1:8003")?,
];

let discovery = Arc::new(StaticDiscovery::new(endpoints));
let lb = Arc::new(LoadBalancer::new(discovery, RoundRobin::new()));
let client = LoadBalancedClient::new(lb, Arc::new(TcpFactory));
client.init().await?;

// Calls are distributed across servers. Setup failures can fail over before invocation.
let response: Response = client.call("method", &request).await?;
client.close().await?;
```

**Strategies:** `RoundRobin`, `Random`, `LeastConnections`, `WeightedRoundRobin`, `ScoreBased`

**Features:**
- Failover for endpoint selection, connection creation, and startup failures
- No automatic retry after RPC invocation, preventing duplicate non-idempotent calls
- Stream affinity (streaming calls stay on same server)
- Failure-based health tracking with configurable thresholds
- DNS-based discovery; periodic refresh and discovery watching are started explicitly

## Documentation

**Reference**

- [Message Protocol](./docs/reference/message-protocol.md) - binary framing and payload encoding
- [Client Lifecycle](./docs/reference/client-lifecycle.md) - start, close, join, and forced shutdown
- [Transport Shutdown](./docs/reference/transport-shutdown.md) - close and terminal error contract

**Migration**

- [Migrating to 0.3](./docs/migration/0.3.md) - lifecycle, error matching, and wire compatibility changes

**Planned:**
- Transport layer guide
- Client/Server usage guide
- Streaming guide
- Discovery & load balancing guide
- Codec guide (Bincode, JSON, MessagePack, CBOR, Postcard)
- Architecture overview & getting started

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

**Planned:**
- `examples/load_balancing.rs` - Discovery and load-balanced RPC
- `examples/streaming.rs` - Server streaming RPC
- `examples/compression.rs` - LZ4/Zstd compression
- `examples/custom_codec.rs` - JSON/MessagePack codec usage

## Used In

- [quant-system](https://github.com/geminik23/quant-system) — A modular workspace for algorithmic trading — real-time market data, strategy execution, and analysis.

## License

MIT License - see [LICENSE](./LICENSE) for details.
