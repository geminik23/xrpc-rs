# xRPC-rs

Fast local RPC library for Rust using shared memory.

**Status:** Very early prototype - It is working but not production-ready.

## Documentation

- [Message Protocol](./docs/message.md) - Binary message format specification

## Future Plans

- [x] add support streaming & compression on message
- [x] Transport layer abstraction
- [x] Local Channel Transport implementation
- [x] SharedMemory Transport implementation
- [x] Heartbeat abstraction for transport reliability
- [ ] DirectTransport (skip Message protocol overhead)
- [ ] ZeroCopyTransport (Arc-based, no serialization)
- [ ] Docs for Transport & Examples


## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
