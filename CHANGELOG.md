# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2026-07-18

### Added

- Added `CallTimeout` and `CallOptions` with `RpcClient::call_with_options()` and `RpcClient::call_raw_with_options()`.
- Added an explicit deadline-free unary-call policy that remains interruptible by client close, terminal connection failure, and local future cancellation.

### Fixed

- `RpcServer::serve()` and `RpcServer::serve_sequential()` now keep healthy sessions active when a retry-safe receive timeout occurs while the message channel remains connected.
- Long-running unary handlers, idle connected sessions, and delayed server streams no longer lose their server session solely because no new inbound message arrived during one transport read interval.
- Deadline-free typed and raw calls release pending registrations and client admission when closed, failed, or dropped, preventing graceful-close deadlocks.

## [0.3.0] - 2026-07-18

### Added

- Added `TransportError::ConnectionClosed` for stable transport shutdown reporting.
- Added `RpcClient::try_start()` for fallible single-start lifecycle management.
- Added `RpcClientHandle::join()` for graceful receive-task completion.
- Added `LoadBalancedClient::close()` for deterministic child-client shutdown.
- Added process-level regression coverage for active 300-second shared-memory reads with joined handles, dropped handles, and dropped clients.
- Added a versioned shared-memory protocol header with readiness, layout, and single-client attachment validation.

### Changed

- Calling RPC methods before startup now fails with `RpcError::ClientError`; calls after close fail with `RpcError::ConnectionClosed`.
- Duplicate and post-close client startup is rejected instead of creating competing receive loops.
- Graceful client close cancels uncommitted sends but waits for committed unary and raw calls to complete or reach their configured timeout.
- The complete graceful client close workflow is single-flight and detached before it waits for admitted work, so cancelling the first close caller cannot abandon shutdown.
- Load-balanced failover retries setup failures only; errors after RPC invocation are not retried because remote commitment is unknown.
- Shared-memory protocol version 2 is incompatible with older mappings and supports one client per named server.
- `RpcError` and `TransportError` are now `#[non_exhaustive]`.
- `UnixFrameTransport::from_stream()` and `TcpFrameTransport::from_stream()` return `TransportResult`.
- The minimum supported Rust version is 1.85.
- Added `socket2` for cross-platform full-duplex socket shutdown.
- Removed the unused `crossbeam` dependency after the in-process transports moved to their own notified queues.

### Fixed

**Transport shutdown**

- Shared-memory reads and writes now observe local close and per-operation cancellation without waiting for their configured I/O timeout.
- Shared-memory send and receive commits are linearized against cancellation and close, preventing abandoned operations from publishing or consuming frames.
- Shared-memory blocking workers now prepare frames without changing ring positions; publication/consumption happens synchronously only after the worker result is ready, eliminating the post-commit cancellation window.
- Shared-memory close is terminal, wakes both directional buffers, prevents reconnect publication, and bounds recovery from an abandoned writer commit.
- Shared-memory attachment now rejects uninitialized, truncated, incompatible, or multiply attached mappings before using shared pointers or events.
- Shared-memory idle timeouts leave healthy transports connected, client heartbeats cover outbound and reconnected buffers, and payload sizing reserves the ring sentinel byte.
- Unix shared-memory waits retry raw-sync's immediate second-boundary `EINVAL` once while preserving repeated or unknown wait failures as `InvalidBufferState`.
- Channel and Arc frame operations now use cancellation-safe notified queues without blocking Tokio workers or leaving detached workers.
- In-process peers can drain frames committed before sender close.
- TCP and Unix close terminates both socket directions and reports peer EOF consistently.
- TCP and Unix reads and writes use independent socket halves while preserving whole-frame serialization.
- Cancelling TCP or Unix I/O after partial frame progress makes the connection terminal instead of leaving framing desynchronized.
- TCP and Unix reject payloads that exceed the 32-bit wire-length prefix.

**RPC lifecycle**

- Idle receive and connected outbound timeouts remain non-terminal.
- Terminal receive and outbound send failures preserve their normalized error for pending calls and streams and close the transport once.
- Pending unary calls, raw calls, stream registrations, and load-balanced affinity are removed when their futures or receivers are cancelled or dropped.
- Forced receive-task shutdown cancels blocked outbound sends.
- Stream chunk sends are serialized with `StreamEnd`; failed or cancelled end sends remain retryable.
- Outbound requests retain the configured codec instance, and stream errors use that configured codec.
- Server-generated unary and streaming error responses use the configured codec instance, including stateful codecs.
- `LoadBalancedClient` retains, closes, and joins child tasks, including clients removed during failover and children drained by a cancelled close caller.
- Load-balanced child close begins concurrently, cached clients are validated against endpoint identity, and dropped calls release active-request accounting.
- Load-balanced streams use one stream ID for affinity and RPC routing, with cleanup scoped to the owning client.

**Message protocol**

- Message length encoding now includes all three variable-field length words and exactly matches the bytes after the 10-byte frame prefix.
- Message decoding validates the declared length and each variable-length field without panicking on malformed input.
- Decoding remains compatible with 0.2.x frames whose declared length omitted the same 10 bytes.

## [0.2.2] - 2026-02-16

### Changed

**Concurrent Serve Loop**

- `RpcServer::serve()` now dispatches each incoming message in its own tokio task instead of processing sequentially. This enables:
  - Multiple streams running simultaneously on the same connection
  - Unary RPCs processed while streams are active
  - Multiple unary RPCs executing concurrently
- All outbound messages are serialized through a single send queue, ensuring safe writes for all transport types (including SharedMemory)

## [0.2.1] - 2026-02-09

### Added

**Layer 4: Service Discovery & Load Balancing**

- `ServiceDiscovery` trait with `StaticDiscovery` and `DnsDiscovery` implementations
- `LoadBalanceStrategy` trait with built-in strategies: `RoundRobin`, `Random`, `LeastConnections`, `WeightedRoundRobin`, `ScoreBased`
- `LoadBalancer` with stream affinity, automatic health tracking, and configurable failure thresholds
- `LoadBalancedClient` with automatic failover and streaming support
- `ClientFactory` trait and `Endpoint` enum for multi-transport endpoint addressing

### Changed

- Removed batching from plan because local IPC is already fast, so batching mainly adds delay without improving throughput.
- Added `rand` for Random load balancing strategy

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
