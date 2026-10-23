# Kill-Gate — tokenstream

## What already exists

**General fan-out servers (Centrifugo, Mercure).** Mature pub/sub over
WebSocket/SSE. Slow-client policy is uniform across all message types: a
bounded per-client queue, and when a consumer cannot keep up the server
disconnects it; the client reconnects and replays missed messages via history /
`Last-Event-ID` (Mercure's `write_timeout` caps how long the hub waits on one
subscriber flush before dropping the connection). Every event is treated
alike — there is no notion that some payloads are safely mergeable and others
are not, no per-client lag budget expressed in events, and no in-band "you fell
behind, here is a snapshot" frame. Sources:
https://centrifugal.dev/docs/pro/rate_limiting ,
https://dev.to/gabrielanhaia/symfony-mercure-server-sent-events-at-scale-without-websockets-4k27

**LLM SSE proxies (the common production pattern).** One upstream stream to
one downstream client: a bounded channel (e.g. 64 events) between reader and
writer goroutines; if the client does not consume within a timeout the stream
is aborted cleanly ("client too slow — abort"). Socket-level coalescing ("when
the socket is not writable, append new text to a pending buffer") is folk
knowledge for a single request/response stream. These are 1:1 proxies, not
1:N fan-out: the question "one slow viewer must not degrade the other viewers
of the same stream" does not arise, and there is no second event source.
Sources: https://dev.to/gauravdagde/streaming-sse-proxying-for-llm-apis-the-hard-parts-4d60 ,
https://www.aicassindra.com/blogs/gpu/gpu_streaming_serve.html ,
https://dev.to/software_mvp-factory/streaming-llm-tokens-to-10k-concurrent-users-3kj2

**Generic SSE backpressure policies** in tutorials and small frameworks:
DROP_NEWEST, DROP_OLDEST, DISCONNECT. Drops are silent — the client cannot
distinguish "nothing happened" from "you missed something" — and disconnect
punts recovery to reconnect/replay machinery.

**Ably "message appends" for AI streaming (closest prior art).** A hosted
pub/sub feature that keeps one in-progress AI response as a single message:
slow consumers receive appends at a capped rate (at most ~25 updates/s), and if
a connection still exceeds its outbound rate limit Ably signals a continuity
loss (error 80020) and the viewer refetches the response so far. This validates
the coalescing instinct, but differs from tokenstream on every axis that
defines this project: it is a managed cloud service, not a buildable local
server; it handles exactly one typed stream (the model's response), not a
dual-source merge of token deltas and independent database change events on
one sequenced stream; the recovery is a client-initiated refetch after an
error, not a server-emitted in-band resync frame carrying the current state
snapshot; and it publishes no measured artifact of per-client lag/TTFT under
adversarial slow clients against a blocking fan-out baseline.
Source: https://ably.com/blog/backpressure-in-ai-streaming

## The differentiation (the claim)

tokenstream is a dual-source streaming server built around a **typed
backpressure policy** and a **per-client lag budget**:

1. One SSE stream per client merges two sources with different loss
   semantics: LLM token deltas (coalescible — merging pending deltas loses
   nothing, the final text is byte-identical) and database change events
   (not coalescible — each is a distinct state transition).
2. Every event carries a per-stream sequence number, so loss is detectable
   by the client, not a matter of trust.
3. Each client has a lag budget in events. Inside the budget, state events
   queue in order. When the budget is exceeded, the server does not drop
   silently and does not disconnect: it collapses everything pending into
   a single **resync frame** that carries the coalesced token text plus a
   full state snapshot stamped with the sequence it is valid at, and the
   stream continues. The contract is mechanical and auditable: every state
   event is either delivered as its own frame or subsumed by a later resync
   whose snapshot provably reflects it.
4. Publishing never blocks on any client (offers are O(1): token text
   appends to a pending batch, the state queue is budget-capped), so
   server memory per client is bounded by one generation's text plus one
   snapshot, independent of how slow the client is.
5. The measured artifact: per-client TTFT and token inter-arrival
   distributions for fast clients with and without adversarial slow/paused
   clients attached, on an identical workload, against a naive fan-out in
   which the publisher writes synchronously to every client (the design
   most hand-rolled SSE servers actually ship).

Distinct from sluice (admission control at the front door): tokenstream is
about fan-out delivery after admission.

## Verdict

**PROCEED.** No available tool is a dual-source, lag-budgeted streaming
server with coalesce-or-resync typed backpressure and an auditable
no-silent-loss contract. The closest feature (Ably appends) is hosted,
single-source, and recovers by client refetch rather than in-band resync.
