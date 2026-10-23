package stream

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// Pump is the per-client backpressure engine. Publishers call Offer, which
// never blocks: token deltas append to a pending batch (coalescing), state
// events queue in order up to the lag budget. When pending state events
// exceed the budget, everything pending collapses into a single resync item
// carrying the coalesced token text and a full state snapshot — nothing is
// dropped silently and the publisher still does not block.
//
// One writer goroutine per pump drains items via Next and acknowledges
// each frame after it is written (Ack), so Lag reflects what the client
// has actually received, not what the server has handed to a blocked
// socket.
type Pump struct {
	mu           sync.Mutex
	notify       chan struct{}
	items        []item
	pendingState int
	pendingBytes int
	budget       int
	snapshotFn   func(uptoSeq uint64) map[string]StateVal

	offered        uint64
	lastOfferedSeq uint64
	deliveredSeq   uint64
	coalesced      uint64 // token deltas merged into an existing pending batch
	resyncs        uint64 // lag-budget collapses
	droppedState   uint64 // state events subsumed by resyncs (never delivered individually)
	framesWritten  uint64
	closed         bool
}

type itemKind int

const (
	itToken itemKind = iota
	itState
	itResync
	itDone
)

type item struct {
	kind         itemKind
	seq          uint64                                   // stream position this item advances the client to
	count        int                                      // source events this item covers
	stateCovered int                                      // resync only: state events subsumed
	bytes        int                                      // approximate wire bytes while pending
	text         string                                   // token batch / resync: coalesced token text
	ev           Event                                    // state only
	snapFn       func(uptoSeq uint64) map[string]StateVal // resync only
	reason       string
}

func (it *item) frame() Frame {
	switch it.kind {
	case itToken:
		return Frame{Type: "token", Seq: it.seq, Text: it.text}
	case itState:
		return FrameFor(it.ev)
	case itResync:
		// The snapshot is resolved lazily, here at drain time: the
		// state as of it.seq is immutable history (initial state plus a
		// log prefix), so resolving it later yields exactly the state
		// the client missed — and Offer never has to call back into the
		// session while holding pump state.
		var snap map[string]StateVal
		if it.snapFn != nil {
			snap = it.snapFn(it.seq)
		}
		return Frame{Type: "resync", Seq: it.seq, Text: it.text, Snapshot: snap, Reason: it.reason}
	case itDone:
		return Frame{Type: "done", Seq: it.seq}
	}
	return Frame{}
}

// NewPump creates a pump. budget is the lag budget in state events:
// pending state events beyond it trigger a resync collapse. snapshotFn
// must return the state exactly as of the given stream sequence.
func NewPump(budget int, snapshotFn func(uptoSeq uint64) map[string]StateVal) *Pump {
	if budget < 1 {
		budget = 1
	}
	return &Pump{
		notify:     make(chan struct{}, 1),
		budget:     budget,
		snapshotFn: snapshotFn,
	}
}

func (p *Pump) signal() {
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// Offer delivers one published event to this client. It is O(1) amortized
// and never blocks on the client's speed.
func (p *Pump) Offer(ev Event) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.offered++
	p.lastOfferedSeq = ev.Seq
	switch ev.Kind {
	case KindToken:
		if n := len(p.items); n > 0 && p.items[n-1].kind == itToken {
			it := &p.items[n-1]
			it.text += ev.Text
			it.seq = ev.Seq
			it.count++
			it.bytes += len(ev.Text)
			p.coalesced++
		} else {
			p.items = append(p.items, item{kind: itToken, seq: ev.Seq, count: 1, bytes: len(ev.Text), text: ev.Text})
		}
		p.pendingBytes += len(ev.Text)
	case KindState:
		b := len(ev.Key) + len(ev.Value) + 48
		p.items = append(p.items, item{kind: itState, seq: ev.Seq, count: 1, bytes: b, ev: ev})
		p.pendingState++
		p.pendingBytes += b
		if p.pendingState > p.budget {
			p.collapseLocked(ev.Seq)
		}
	case KindDone:
		p.items = append(p.items, item{kind: itDone, seq: ev.Seq, count: 1})
	}
	p.mu.Unlock()
	p.signal()
}

// collapseLocked folds every pending item into one resync item: token
// text concatenated in order (byte-preserving), state events subsumed by
// a snapshot taken as of uptoSeq. Done items (only possible at the tail)
// are kept after the resync. Caller holds p.mu.
func (p *Pump) collapseLocked(uptoSeq uint64) {
	var text strings.Builder
	var dones []item
	stateCovered := 0
	for _, it := range p.items {
		switch it.kind {
		case itToken, itResync:
			text.WriteString(it.text)
			stateCovered += it.stateCovered
		case itState:
			stateCovered++
			p.droppedState++
		case itDone:
			dones = append(dones, it)
		}
	}
	rs := item{
		kind:         itResync,
		seq:          uptoSeq,
		stateCovered: stateCovered,
		text:         text.String(),
		snapFn:       p.snapshotFn,
		reason:       fmt.Sprintf("lag budget exceeded: %d state events pending (budget %d)", stateCovered, p.budget),
	}
	// Pending-bytes accounting uses a fixed snapshot allowance: the
	// snapshot itself is only materialized at drain time.
	rs.bytes = len(rs.text) + 256
	rs.count = stateCovered // informational; seq carries the coverage semantics
	p.items = append([]item{rs}, dones...)
	p.pendingState = 0
	p.pendingBytes = rs.bytes
	for _, d := range dones {
		p.pendingBytes += d.bytes
	}
	p.resyncs++
}

// Next pops the oldest pending item as a frame, blocking until one is
// available, the context ends, or the pump is closed and drained.
func (p *Pump) Next(ctx context.Context) (Frame, bool) {
	for {
		p.mu.Lock()
		if len(p.items) > 0 {
			it := p.items[0]
			p.items = p.items[1:]
			if it.kind == itState {
				p.pendingState--
			}
			p.pendingBytes -= it.bytes
			if p.pendingBytes < 0 {
				p.pendingBytes = 0
			}
			p.mu.Unlock()
			return it.frame(), true
		}
		if p.closed {
			p.mu.Unlock()
			return Frame{}, false
		}
		p.mu.Unlock()
		select {
		case <-p.notify:
		case <-ctx.Done():
			return Frame{}, false
		}
	}
}

// Ack records that a frame was fully written to the client.
func (p *Pump) Ack(f Frame) {
	p.mu.Lock()
	if f.Seq > p.deliveredSeq {
		p.deliveredSeq = f.Seq
	}
	p.framesWritten++
	p.mu.Unlock()
}

// Close stops the pump: Offer becomes a no-op and Next drains what is
// left, then reports exhaustion.
func (p *Pump) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.signal()
}

// Stats is a point-in-time snapshot of pump counters.
type Stats struct {
	Offered        uint64 `json:"offered"`
	DeliveredSeq   uint64 `json:"delivered_seq"`
	LastOfferedSeq uint64 `json:"last_offered_seq"`
	LagEvents      uint64 `json:"lag_events"`
	Coalesced      uint64 `json:"coalesced"`
	Resyncs        uint64 `json:"resyncs"`
	DroppedState   uint64 `json:"state_subsumed_by_resync"`
	FramesWritten  uint64 `json:"frames_written"`
	PendingItems   int    `json:"pending_items"`
	PendingBytes   int    `json:"pending_bytes"`
}

func (p *Pump) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{
		Offered:        p.offered,
		DeliveredSeq:   p.deliveredSeq,
		LastOfferedSeq: p.lastOfferedSeq,
		LagEvents:      p.lastOfferedSeq - p.deliveredSeq,
		Coalesced:      p.coalesced,
		Resyncs:        p.resyncs,
		DroppedState:   p.droppedState,
		FramesWritten:  p.framesWritten,
		PendingItems:   len(p.items),
		PendingBytes:   p.pendingBytes,
	}
}
