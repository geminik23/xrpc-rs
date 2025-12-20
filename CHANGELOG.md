# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned

- Batching support
- Service Discovery & Load Balancing

## [0.2.0] - 2025-12-20

### Changed

**Transport Renaming & Restructuring**

Layer 1 (FrameTransport):

- `Transport` trait -> `FrameTransport` (methods: `send_frame`, `recv_frame`)
- `TcpTransport` -> `TcpFrameTransport`
- `TcpTransportListener` -> `TcpFrameTransportListener`
- `UnixTransport` -> `UnixFrameTransport`
- `UnixTransportListener` -> `UnixFrameTransportListener`
- `ChannelTransport` -> `ChannelFrameTransport`
- `SharedMemoryTransport` -> `SharedMemoryFrameTransport`
- `ArcTransport` -> `ArcFrameTransport`

Layer 2 (Channel):

- `MessageTransport` trait -> `MessageChannel`
- `MessageTransportAdapter` -> `MessageChannelAdapter`
- `RawTransport` -> `SerdeChannel`
- Moved Layer 2 components to new `channel/` module

### Added

- `ZeroCopyTransport` trait for `ArcFrameTransport` (typed zero-copy API)
- Deprecated aliases for backward compatibility

### Deprecated

- Old names (`Transport`, `TcpTransport`, `MessageTransportAdapter`, etc.) - use new names

## [0.1.12] - 2025-12-20

### Added

- `MessagePackCodec` (feature: `codec-messagepack`)
- `CborCodec` (feature: `codec-cbor`)
- `PostcardCodec` (feature: `codec-postcard`)

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


## [0.1.10] - 2025-12-13

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
