# tokenstream: typed backpressure for dual-source AI streams

## Problem

An AI product stream is rarely just tokens. The same screen that renders
a model's answer also renders the state the answer depends on — a
patient status, a job's progress, a document version — changing while
the tokens flow. Serving both on one connection creates a fan-out
problem with a twist: the two event kinds have opposite loss semantics,
and the standard policies ignore that.

- Block on the slowest client → one frozen tab degrades every viewer.
- Buffer unboundedly → slow clients become a memory attack.
- Drop when behind (DROP_OLDEST / DROP_NEWEST) → state changes vanish
  silently; the client cannot tell "nothing changed" from "you missed
  it".
- Disconnect when behind → recovery becomes reconnect/replay machinery,
  and the in-progress answer is exactly what replay handles worst.

## Prior art (summary; full analysis in KILLGATE.md)

Centrifugo and Mercure fan out uniformly-typed messages with bounded
queues, disconnecting consumers that cannot keep up and relying on
history / `Last-Event-ID` replay. LLM SSE proxies are 1:1 — a bounded
channel and "abort if the client is too slow" — so the fan-out question
never arises. Ably's message appends for AI streams come closest:
appends to one response message are rate-capped for slow consumers and a
continuity-loss error tells the client to refetch. That is a hosted,
single-source feature with client-initiated recovery — not a local
dual-source server with in-band resync.

## Design

### Events and sequence

A session is a single ordered stream. The hub assigns each published
event a gapless per-session sequence number and appends it to a source
log (the ground truth for audits). Two sources publish into the same
sequence space: the token generator and the store (every `Set` is a
state event on every live session).

### The pump: per-client backpressure

Each subscription gets a `Pump`. Publishers call `Offer`, which is O(1)
and never waits on the client:

- A token delta appends to the pending token batch if one is last in
  the queue (coalescing), else starts a new batch.
- A state event enqueues in order. If pending state events exceed the
  client's lag budget, the pump collapses: all pending items fold into
  one resync item — token texts concatenated in order, state events
  counted as subsumed, snapshot deferred to drain time (below).
- A done event queues last.

A writer goroutine per pump drains items, writes SSE frames, and only
then acknowledges them, so the pump's lag counter (last offered seq −
last acknowledged seq) measures what the client has actually received.

Two subtleties mattered:

1. **Ordering under concurrent publishers.** Offers happen under the
   session lock, immediately after sequence assignment, so every pump
   sees events in strict sequence order even when a token generator and
   a burst writer publish concurrently. This is safe because `Offer`
   never blocks.
2. **Snapshots must be as-of their sequence.** A resync snapshot read
   from the live store could include a write whose event carries a
   *higher* sequence than the resync's (the write raced ahead of its
   publish). tokenstream therefore computes snapshots from the session's
   initial state plus the log prefix up to the stamped sequence — and
   resolves them lazily at drain time, when no locks are held. The
   snapshot at seq S reflects exactly events ≤ S, always.

### The audit

`stream.Verify(log, initial, frames)` re-derives everything a client
should have ended with and checks: strictly increasing frame seqs;
byte-identical reconstructed text; every hello/resync snapshot exactly
equal to the state as of its seq; exact final state; and the
no-silent-loss contract — each log state event either delivered as a
frame or covered by a resync (or hello) at a ≥ seq with a consistent
snapshot. The naive baseline's output goes through the same verifier.

## Experiment

Identical workload in all scenarios: 300 tokens from the deterministic
generator (seed 42), 12 state writes interleaved every 25 tokens, and a
240-event state burst fired concurrently at generation start — 553
events total. Lag budget 48 state events. Clients: two fast (record
instantly), one slow (paced at 1 byte / 60 µs ≈ 16.6 KB/s), one paused
(writes block for 1,500 ms). Scenarios: tokenstream fast-only control;
tokenstream mixed; naive blocking fan-out mixed (no paused client —
blocking on a stalled client for 1.5 s adds nothing the slow client
doesn't already show). Achieved generator pacing on the 2-core test VM
was ~1.1 ms median inter-token (requested 2,000/s); all scenarios ran
on the same box under the same conditions. Numbers below are from
`results.json` (a repeat run reproduced every conclusion; p95 values
agreed within ~0.5 ms).

### Results

Fast clients, tokenstream, control vs mixed: TTFT 0.48–0.51 ms vs
0.30–0.37 ms; token inter-arrival p50 1.10 ms vs 1.09 ms; p95 2.16 ms
vs 1.58 ms; completion 358 ms vs 326 ms. **Attaching a slow and a
stalled client did not measurably degrade fast clients** — differences
are within run jitter.

Fast clients, naive fan-out, same slow client attached: TTFT 11.34 ms,
p50 5.20 ms, **p95 17.91 ms (≈11× tokenstream-mixed)**, completion
3,111 ms. The naive publish loop took **3,116 ms** to emit the workload
that tokenstream's generator emitted in 326 ms on its own schedule:
every publish waited for the slow client's pacing.

Slow client, tokenstream: 553 events arrived as 72 frames; 288 of 300
deltas coalesced; token inter-arrival p95 23.3 ms (its own pacing);
complete at 583 ms, ~257 ms after generation ended. Final text
byte-identical; audit passed.

Paused client, tokenstream: everything collapsed while it was stalled —
288 deltas coalesced, 245 state events subsumed by resyncs — and it
received the entire stream as 18 frames, completing at 1,499.9 ms,
i.e. the moment the stall ended. Audit passed.

Memory: peak pending data summed over all four clients during the
mixed run was **6,601 bytes**; peak lag was 553 events (the paused
client, a whole finite generation behind — the bound is the generation
size plus one snapshot, by construction). Process RSS grew ≈0.5 MB
across the mixed scenario, harness bookkeeping included.

Audits: all 7 client streams across the 3 scenarios passed `Verify` —
byte-identical text, exact final state, zero silent loss. The naive
baseline also passes the audit (it loses nothing); its failure is
purely latency, which is the point of the contrast. A unit test deletes
one state frame from a valid stream and confirms `Verify` fails with a
silent-loss error, so the audit is evidence, not decoration.

### The burst wrinkle

Fast clients resynced 2–4 times per run, subsuming 98–196 state events,
because the 240-event burst arrives faster than any writer goroutine
drains and the budget counts events, not time. The policy's stance:
when the queue is the bottleneck, a snapshot is strictly better than a
long queue — the client gets the same final state sooner, with proof.
A time-based budget (e.g. "state events younger than X ms queue
normally") would reduce resyncs for fast clients at the cost of a less
predictable memory bound; noted as future work, not hidden.

## Tests

7 tests, all passing under `-race`:

- coalescing preserves final text exactly (100 deltas → 1 frame);
- lag-budget breach collapses to a resync carrying the snapshot, with
  exact subsumption counts across repeated breaches;
- a resync carries pending token text verbatim, in order;
- end-to-end audit passes for a fast client and a client stalled for
  the entire workload, and the publisher never blocks on the stalled
  one;
- the audit detects a silently dropped state frame and a forged state
  value;
- frame seqs stay strictly increasing under concurrent publishers;
- the real HTTP path (httptest): hello-first SSE stream reconstructs
  the session text exactly; dashboard and store/snapshot endpoints
  agree.

## Limits

Single node; in-memory store and logs; SSE only; no heartbeats (dead
clients are reaped by a 30 s write deadline); lag budget counts events
rather than bytes or time; the LLM is a deterministic local mock, so
upstream provider jitter is out of scope. The workload is one session
with four clients by design — the claim is per-client isolation and
bounded memory, which are per-session properties; multi-session scale
testing is future work.
