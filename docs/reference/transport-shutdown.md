# Transport shutdown contract

All frame transports expose terminal local and peer close consistently.

| Event | Transport result | RPC mapping |
|---|---|---|
| Explicit local close | `ConnectionClosed` for later operations | `ConnectionClosed` |
| Clean peer close at a frame boundary | `ConnectionClosed` | `ConnectionClosed` |
| Peer closes during a frame | `Protocol` | `Transport(Protocol)` |
| Timeout before consuming or publishing a frame | `Timeout` while still open | `Transport(Timeout)` |
| Timeout after partial socket framing | `Timeout`, then terminal | `Transport(Timeout)` |
| Connection absent before terminal close | `NotConnected` | `Transport(NotConnected)` |
| Reconnect attempts exhausted | `ConnectionFailed` | `Transport(ConnectionFailed)` |

## Shared memory

SHM operations use cooperative cancellation around blocking raw-sync waits. The blocking worker only prepares an operation: it waits and copies bytes, but does not change `write_pos` or `read_pos`. After the worker result is ready, the async-side poll checks terminal/cancellation state and publishes or consumes the ring position synchronously. There is no await between that commit and the public future returning success.

Send and receive use explicit operation states so cancellation while preparation is pending cannot publish `write_pos` or consume `read_pos`. Once an operation wins the commit transition, close waits for the small position-publication critical section. A complete committed frame remains readable after peer close.

The shared mapping uses a versioned protocol header and publishes readiness only after the complete layout and both events are initialized. Attachment validates the mapping length, magic, protocol version, readiness, capacity, control size, event size, and total layout before using shared pointers. Version 0.3 uses SHM protocol version 2 and does not attach to older mappings.

Each named SHM server supports one attached client. The two directional buffers are SPSC queues; a second client is rejected instead of allowing multiple writers to corrupt a frame. Restart all participating processes when upgrading so old mappings are recreated.

Close prevents reconnect publication, wakes both directional events, and bounds recovery from an abandoned writer commit. If a writer process dies inside the commit section, close forces terminal state after 100 ms and returns `InvalidBufferState` rather than waiting forever.

A timeout before frame commit keeps the transport connected and allows a later operation. Payload validation reserves the ring sentinel byte, so the largest SHM payload is `buffer_size - 5` bytes.

The raw-sync Unix backend reports a generic wait failure for pthread condition errors and does not normalize `tv_nsec` when a timeout crosses a real-time second boundary. xrpc-rs accepts the generic message as timeout only when the native wait consumed the requested interval. An immediate generic failure is retried once across the clock boundary; a repeated or unknown failure remains `InvalidBufferState`.

## TCP and Unix sockets

Socket transports use independent owned read and write halves. Sends serialize a complete length header and payload; receives independently serialize a complete header and payload. This permits full-duplex RPC without interleaving frame bytes.

Close shuts down both socket directions without waiting for either half's mutex. Dropping or aborting an operation after any frame byte has progressed also makes the connection terminal and shuts down both directions, preventing a later operation from reusing a corrupted frame boundary. Cancellation before byte progress leaves the connection reusable.

A clean EOF before a new frame is `ConnectionClosed`. Partial-frame EOF is `Protocol`. A partial-frame timeout returns `Timeout` and makes the connection terminal. Outbound payloads are limited by both `max_message_size` and the `u32` wire-length prefix.

## In-process transports

Channel and Arc frame operations use bounded queues with Tokio `Notify`; they do not block runtime workers or leave detached blocking workers after cancellation. A pending future that is dropped cannot later publish or consume a frame.

Close state is endpoint-local and peer-aware. The peer may drain frames committed before sender close, then receives `ConnectionClosed`. Arc's synchronous zero-copy API remains blocking by design but follows the same close and committed-frame rules.

## Related documentation

- [Message protocol](./message-protocol.md)
- [Client lifecycle](./client-lifecycle.md)
- [Migrating to 0.3](../migration/0.3.md)
