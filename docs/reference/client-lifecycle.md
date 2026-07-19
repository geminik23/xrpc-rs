# RPC client lifecycle

An `RpcClient` starts in the not-started state. Calling `try_start()` exactly once is required before making RPCs. It creates the receive loop and returns a handle that owns explicit join and forced-shutdown control.

Retaining the handle is not required for ordinary RPC operation. Dropping it detaches join ownership without stopping the receive loop. Retain it when the application needs to confirm that the receive task has terminated or may need forced shutdown.

```mermaid
stateDiagram-v2
    [*] --> NotStarted
    NotStarted --> Running: try_start()
    NotStarted --> Closed: close() or drop
    Running --> Closing: close() or terminal I/O failure
    Running --> Closed: drop or forced abort
    Closing --> Closed: admitted work and transport close finish
    Closed --> [*]
```

## Graceful close

`close()` marks the client terminal before waiting for admission. New work and uncommitted blocked sends fail with `ConnectionClosed` immediately.

A finite unary or raw RPC whose transport send already returned success is treated as committed. Its admission guard remains active until the response arrives or the call timeout expires, so explicit close cannot report a committed non-idempotent RPC as `ConnectionClosed`. This means graceful close can wait up to the remaining timeout of committed finite calls.

A call using `CallOptions::without_timeout()` has no application response deadline, so it observes the client's existing close/terminal notification after its send commits. Explicit close or terminal connection failure ends that local wait, removes the pending request, and releases admission so close can continue. This does not send request cancellation to the server.

The complete graceful-close workflow is claimed and spawned before its first admission wait. After admitted work exits, that detached workflow stops the receive loop, completes pending local work, starts transport close exactly once, and stores the result. Cancelling the caller while it is still waiting for admission therefore does not abandon shutdown. Concurrent, cancelled, and later close callers all observe the same stored result.

`close()` completes the graceful close workflow without requiring the public handle. If the handle was retained, call `RpcClientHandle::join()` afterward to confirm that the receive task itself has terminated.

## Call response deadlines

`RpcClient::call()` and `RpcClient::call_raw()` use the default response timeout configured on the client. The default is 30 seconds unless another value was supplied to `with_timeout()` or `with_codec_and_timeout()`.

Use `CallOptions` when the timeout policy must be explicit:

```rust
use std::time::Duration;
use xrpc::CallOptions;

// Resolve to the timeout configured on RpcClient.
let default_response: Response = client
    .call_with_options("method", &request, CallOptions::default())
    .await?;

// Override the client default for this call.
let bounded_response: Response = client
    .call_with_options(
        "method",
        &request,
        CallOptions::with_timeout(Duration::from_secs(2)),
    )
    .await?;

// Apply no application-level response deadline.
let unbounded_response: Response = client
    .call_with_options("method", &request, CallOptions::without_timeout())
    .await?;
```

The typed and raw options APIs have the same lifecycle behavior. `CallTimeout::Disabled` does not disable transport send/read timeouts and does not make the call immune to client close, terminal connection failure, or local future cancellation. A response that is already ready wins if it is observed at the same time as close.

Timeout, close, failure, and dropping the call future remove the local pending registration. None of these events cancel a handler already executing on the server; use a separate application cancel RPC when remote cancellation is required.

## Receive and send failures

A retry-safe receive timeout is non-terminal while the transport remains connected. The receive loop continues waiting instead of closing an idle client. A timeout after partial framing must make the channel terminal and is not retried.

A terminal receive error or terminal outbound send error marks the client closed, cancels uncommitted sends, stops the receive loop, and completes pending calls and streams with the normalized original error. Transport close remains single-flight when explicit close races with either failure path.

The client assumes that `MessageChannel::send()` returning `Ok(())` is the send commit boundary. A channel implementation must not publish data after its pending send future has been cancelled. The built-in SHM transport performs blocking wait/copy as preparation only; it publishes or consumes the ring position synchronously after the blocking result has become ready, with no further await before returning success.

## Forced shutdown and drop

`RpcClientHandle::shutdown()` aborts and joins the receive task. The receive-task finalizer also cancels blocked outbound sends and drains pending calls and streams. Use forced shutdown only when graceful close cannot complete.

Dropping `RpcClient` aborts its receive task through an internally retained abort handle and drains local waiters on a best-effort basis. Drop cannot report transport errors and is not a substitute for explicit `close()`. For strict task-completion confirmation, retain the public handle and follow `close()` with `join()`.

## Streaming

Stream registrations are owned by `StreamReceiver` and are removed on normal `StreamEnd`, stream error, initial send failure, cancellation, client close, or receiver drop.

`StreamSender` serializes chunk sends with `end()`. The ended state is published only after the `StreamEnd` message commits, so a failed or cancelled end can be retried and no later chunk can commit after a successful end.

Load-balanced stream affinity follows the lifetime of `StreamReceiver`, including normal completion, error, cancel, and drop.

## Related documentation

- [Message protocol](./message-protocol.md)
- [Transport shutdown](./transport-shutdown.md)
- [Migrating to 0.3](../migration/0.3.md)
