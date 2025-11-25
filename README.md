# xRPC-rs

Fast RPC library for Rust with multiple transport implementations.

This library provides efficient message passing with various transports including in-process channels, shared memory for IPC, and planned support for network transports (TCP, etc.). While currently focused on local communication, the architecture is designed to support both local and remote RPC scenarios.

**Status:** Very early prototype - It is working but not production-ready.

## Documentation

- [Message Protocol](./docs/message.md) - Binary message format specification

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

- [ ] Documentation for Transports & Examples
- [ ] Advanced network transports (TCP, etc.) ...


## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
