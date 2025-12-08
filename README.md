# xRPC-rs

High-performance local IPC library for Rust with seamless transport flexibility.

Designed for multi-process applications on the same machine. Start with in-process channels for development, scale to shared memory for production IPC, and extend to TCP for network deployment—all with the same interface. 

**Status:** Very early prototype - It is working but not production-ready.

## Architecture

xRPC follows a layered architecture:

| Layer | Trait/Module | Description | Status |
|-------|--------------|-------------|--------|
| Layer 1 | `Transport` | Low-level byte transmission (TCP, Unix, SharedMemory, Channel) | completed |
| Layer 2 | `MessageTransport` | Message-aware transport with compression | completed |
| Layer 3 | `RpcClient/RpcServer` | RPC with method dispatch, streaming, request/response correlation | implementing |
| Layer 4 | Advanced | Batching, service discovery, load balancing | not implemented |

```
Application
    ↓
RpcClient/RpcServer (Layer 3)
    ↓
MessageTransport (Layer 2) with compression
    ↓
Transport (Layer 1) raw bytes with framing
    ↓
Network/IPC
```

## Documentation

- [Message Protocol](./docs/message.md) - Binary message format specification

## Examples

### Byte-level Transports

Layer 1 transports (Channel, TCP, RawTransport, TypedChannel):

```bash
cargo run --example byte_transports
```

### MessageTransport with SharedMemory

Local RPC using Message protocol over shared memory:

```bash
# Terminal 1: Start server
cargo run --example message_transport_shm -- server

# Terminal 2: Run client
cargo run --example message_transport_shm -- client
```

### RPC Client/Server

High-level RPC client and server with typed handlers:

```bash
# Terminal 1: Start server
cargo run --example rpc_client_server -- server

# Terminal 2: Run client
cargo run --example rpc_client_server -- client
```


## Future Plans

### Completed

- [x] Add support for streaming & compression on message
- [x] Transport layer abstraction
- [x] Local Channel Transport implementation
- [x] SharedMemory Transport implementation
- [x] Heartbeat abstraction for transport reliability
- [x] RawTransport (skip Message protocol overhead)
- [x] ArcTransport (Arc-based, no serialization)

### Refactoring Tasks

- [x] Module reorganization: Split `protocol.rs` into `message` module.
- [x] Codec trait abstraction: Decouple serialization format
- [x] Relocate `TypedChannel`

### Planned Features

- [x] Network transports (TCP, Unix socket)
- [x] MessageTransport (Layer 2) - Message-aware transport adapter
- [x] Examples for Transports
- [x] High-level Client/Server architecture (Layer 3)
- [ ] Streaming support
- [ ] Connection pooling
- [ ] Batching support
- [ ] Service Discovery & Load Balancing


## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
