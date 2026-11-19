# Penpot 2.18: concurrent worker connections

This fork keeps the deployed 2.18 protocol and JSON response patch. Upstream
PR 12492 remains separate and targets develop/SDK2. This release does not
upgrade the frontend, backend, database or document format.

The server accepts 30 distinct plugin sessions per user when configured:

```yaml
environment:
  PENPOT_MCP_JSON_RESPONSE: "true"
  PENPOT_MCP_MAX_PLUGIN_CONNECTIONS_PER_USER: "30"
  PENPOT_MCP_MAX_PENDING_TASKS_PER_PLUGIN: "30"
  PENPOT_MCP_QUEUE_TIMEOUT_S: "120"
  PENPOT_MCP_TOOL_TIMEOUT_S: "120"
  PENPOT_MCP_LIVENESS_PROBE_TIMEOUT_S: "5"
```

The connection limit defaults to 1. Queue limits count the active task and
waiting tasks together. Invalid limits fail startup. Each editor executes one
task at a time; different editors run in parallel. The queue is bounded and
is held in memory. It is not a durable job scheduler.

## Existing single editor

Thirty independent MCP clients can keep their existing token URL and share
the existing editor. No plugin or frontend update is required. Tool calls to
that editor execute serially. Jobs must still own their assigned file/page;
a queue serializes individual calls, not a multi-call transaction.

## Separate editors

Use a distinct `pluginSessionId` for each owned editor, on both endpoints:

```text
/mcp/stream?userToken=<existing-token>&pluginSessionId=worker-01
/mcp/ws?userToken=<existing-token>&pluginSessionId=worker-01
```

The selector is not a credential. It is scoped by the existing user token.
An initialized HTTP/SSE session keeps its original selector. A missing selector
targets only the legacy, unnamed plugin; it never picks a named editor by
chance. A duplicate name or a connection over the configured limit is rejected
without evicting the existing connection. The exception is an abandoned
registration: when the conflicting connection holds no active, queued or
probe task and its socket is closed or silent beyond the heartbeat threshold
(30 s), it is released so a reloaded or crashed editor can reconnect at once.
A connection that still holds work is never released this way.

The 2.18 integrated plugin does not supply this property itself. An owned
Playwright context can use `operators/worker-connection.cjs` **before opening
the editor**. It adds the selector only to the exact configured MCP socket
and returns the matching private MCP URL. It preserves the existing token;
it does not create identities or rotate credentials. Do not log either URL.
The adapter normalizes HTTP(S) and relative WebSocket addresses to WS(S)
before comparing the endpoint. Penpot 2.18 passes an HTTPS address to the
browser's WebSocket constructor, so this normalization is required for routing.
Existing single-editor workers need no adapter.

Separate editors must have separate files or an external file ownership lock.
The server cannot infer file ownership from arbitrary JavaScript tool calls.
Thirty connections do not authorize 30 model processes or provision browser
RAM; the worker launcher must enforce company limits and memory reservations.

## Failure behavior

Disconnect rejects active work with an unknown-outcome error and waiting work
with a not-dispatched error. Execution timeout closes that plugin connection
and cancels its queue. A new connection does not replay those operations.
Reconcile saved IDs after an unknown outcome before retrying a mutation.
MCP request cancellation and session termination remove queued work before dispatch.
Cancelling an already running request keeps that editor occupied until the plugin
finishes or times out; it cannot stop JavaScript already executing.
Queue overflow and queue wait expiry reject before sending the task. Tool
failures set MCP `isError: true`. Responses can only settle the active task
owned by the responding socket.

An idle connection with an old heartbeat gets one read-only `return null;`
probe before any requested operation is sent. The exact socket must reply
within the configured probe timeout. This checks the browser JavaScript event
loop, unlike a WebSocket pong, and lets responsive tabs recover from delayed
heartbeat timers. Explicit freeze signals still reject work. A failed probe
cancels queued operations as not dispatched; it never sends their code.

The production deployment uses one MCP server and no Redis routing. This
release's 30-worker acceptance is for that topology; multi-server Redis
ownership and failover are not qualified by these tests.

## Verification

`MultiClient.test.ts` uses real HTTP sessions and synthetic WebSocket plugins:
30 plugin sessions / 300 concurrent calls, 30 clients sharing one serialized
legacy editor, owner isolation, duplicate/capacity rejection, immutable routing,
disconnect, timeout, queue expiry/overflow, and response ownership. These are
transport tests, not acceptance of 30 completed design jobs.

Run `node --test operators/worker-connection.test.cjs` for the browser address
variants, exact endpoint isolation, and owner-mismatch adapter regressions.
