# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned

- Batching support
- Service Discovery & Load Balancing

## [0.1.12] - 2025-01-20

### Fixed

- SharedMemoryTransport 
  - ring buffer pointer wrapping
  - auto-reconnect buffer replacement
  - config timeouts
- Streaming 
  - flag for StreamChunk/StreamEnd messages
  - Server streaming errors does not route to client

### Changed

- Streams assume ordered transport (removed unused reordering buffer)


## [0.1.0] - 2025-01-01

Initial release with core functionality.

### Added

**Layer 1: Transport**

- `Transport` trait
- `ChannelTransport`
- `SharedMemoryTransport`
- `TcpTransport`
- `UnixSocketTransport`
- `ArcTransport`
- `RawTransport`
- `TypedChannel`
- `TransportStats`
- Retry policies (Fixed, Linear, Exponential)
- Heartbeat mechanism

**Layer 2: MessageTransport**

- `MessageTransport` trait
- `MessageTransportAdapter`
- Compression (LZ4, Zstd)

**Layer 3: RPC**

- `RpcClient`
- `RpcServer`
- `StreamManager`, `StreamSender`, `StreamReceiver`
- `ServerStreamSender`
- `ConnectionPool`

**Protocol**

- `Message` struct
- `MessageType` (Call, Reply, Notification, Error, StreamChunk, StreamEnd)
- `Codec` trait (`BincodeCodec`, `JsonCodec`)
- `MessageMetadata`
