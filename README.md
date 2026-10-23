# tokenstream

A real-time streaming server in Go (stdlib only) that merges **two sources
on one SSE stream per client** — live LLM token deltas and database change
events — under a **typed backpressure policy with a per-client lag budget**,
so a slow or stalled client can never hurt a fast one, and nothing is ever
silently dropped.

## The novel position

Streaming servers treat all events alike: when a client falls behind they
either block the publisher (hurting every other client), buffer without
bound (OOM), drop frames silently, or disconnect the client. But the two
kinds of events on an AI stream have opposite loss semantics:

- **Token deltas are coalescible.** If a client is behind, pending deltas
  can be merged into one frame; the reconstructed final text is
  byte-identical. Dropping is never necessary.
- **State-change events are not coalescible.** Each is a distinct state
  transition. It must be delivered — or explicitly superseded.

tokenstream encodes exactly that distinction, plus a contract that makes
loss *auditable* rather than a matter of trust:

1. Every event carries a per-stream sequence number; frame seqs on a
   connection are strictly increasing.
2. Each client has a **lag budget** (pending state events, default 64).
   Inside the budget, state events queue in publish order.
3. When the budget is exceeded, the server collapses everything pending
   into a single **resync frame**: the coalesced token text, verbatim,
   plus a full state snapshot stamped with the sequence it is valid at.
   The stream continues from there. No disconnect, no silent drop.
4. Publishing **never blocks on a client**: offers are O(1) (token text
   appends to a pending batch; the state queue is budget-capped), so
   per-client server memory is bounded by one generation's text plus one
   snapshot, no matter how slow the client is.
5. A client that joins late gets a `hello` frame — text so far plus the
   state exactly as of the current sequence — then live events.

The audit contract: for any client, every state event in the source log
was either delivered as its own frame or subsumed by a resync whose
snapshot provably reflects it. `stream.Verify` checks this mechanically
against the log, along with byte-identical final text and exact final
state. A test proves the audit is not vacuous: deleting one state frame
fails it.

## Measured results

Workload (identical across scenarios): 300 tokens, 12 interleaved state
writes, and a 240-event state burst at generation start — 553 events.
Lag budget 48 state events. Slow client reads at ~16.6 KB/s (1 byte per
60 µs); paused client is fully stalled for 1,500 ms. Achieved generator
pacing on this 2-core VM: ~1.1 ms median inter-token. Full data:
`results.json`; method and discussion: `PAPER.md`.

| scenario / client | TTFT | token p50 | token p95 | stream done |
|---|---|---|---|---|
| tokenstream, fast only (control) | 0.48–0.51 ms | 1.10 ms | 2.16 ms | 358 ms |
| tokenstream + slow + paused attached, **fast clients** | 0.30–0.37 ms | 1.09 ms | **1.58 ms** | 326 ms |
| tokenstream, slow client | 23.2 ms | 20.3 ms | 23.3 ms | 583 ms |
| tokenstream, paused client | 1,499.8 ms | — | — | 1,499.9 ms |
| **naive blocking fan-out, fast clients** | 11.3 ms | 5.20 ms | **17.9 ms** | 3,111 ms |

- Fast clients with a slow and a stalled client attached are
  indistinguishable from the fast-only control (p95 1.58 ms vs 2.16 ms —
  no degradation; the difference is run jitter). Against the naive
  fan-out — publisher writes synchronously to every client — the same
  fast clients degrade ~11× at p95 and take 9.6× longer to finish; the
  naive publisher itself was blocked for 3,116 ms vs 326 ms.
- The slow client received 553 events as **72 frames**: 288 of 300 token
  deltas were coalesced away, and its final text is byte-identical.
- The paused client received the whole stream as **18 frames** and was
  fully caught up the moment it resumed (done at 1,499.9 ms against a
  1,500 ms stall): 245 state events were subsumed by resync snapshots.
- Peak pending data across **all** clients during the mixed run:
  **6,601 bytes**. Process RSS grew ~0.5 MB during the run.
- Every client in every scenario passed the audit: byte-identical final
  text, exact final state, zero silently lost state events.

One honest wrinkle: during the 240-event burst, even fast clients resync
(2–4 times) — the budget counts events, and the burst arrives faster than
any writer goroutine drains. Correctness is unaffected (state arrives via
snapshot); the policy deliberately prefers a snapshot over a long queue
whenever the queue, not the client, is the bottleneck.

## Run it

```sh
go run ./cmd/tokenstream -addr :8080 -budget 64
# open http://localhost:8080 — start a stream, write to the store,
# watch tokens and state merge on one connection.

curl -X POST localhost:8080/sessions -d '{"prompt":"demo","tokens":240,"rate":40}'
curl -N localhost:8080/sessions/s-1/stream
curl -X POST localhost:8080/store -d '{"key":"status","value":"critical"}'
curl localhost:8080/sessions/s-1/snapshot
curl localhost:8080/metrics
```

Reproduce the measurements:

```sh
go run ./cmd/experiment -out results.json
go test ./...   # 7 tests, race-clean
```

## Layout

- `internal/stream` — events, store, hub/session (sequence + source log),
  the per-client backpressure `Pump`, the token generator, and `Verify`,
  the client-side audit.
- `internal/naive` — the blocking fan-out baseline (publisher writes to
  every subscriber synchronously; no buffer, no policy).
- `internal/harness` — the workload: fast/slow/paused sinks, the three
  scenarios, per-client TTFT / inter-arrival / completion metrics.
- `internal/server` — HTTP API + embedded dashboard (plain JS
  EventSource, no build step).

## Honest limits

- Single node, in-memory store and session logs; no persistence, no
  cross-process fan-out.
- SSE only (no WebSocket); no heartbeat frames yet, so idle streams rely
  on the 30 s write deadline to reap dead clients.
- The lag budget counts state events, not bytes or time; a workload of
  very large state values would want a byte budget too.
- Token coalescing preserves the final text exactly, but a client that
  renders partial text sees it in larger chunks when behind — that is
  the intended trade.
- The mock LLM is deterministic and local; a real provider adds network
  jitter upstream of the hub, which the hub isolates clients from but
  cannot remove.
