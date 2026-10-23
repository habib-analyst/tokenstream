// Package naive is the honest baseline: the fan-out most hand-rolled SSE
// servers actually ship. The publisher formats each event and writes it
// to every subscriber synchronously, under one lock. There is no buffer
// and no policy: the slowest subscriber gates the publisher, and
// therefore gates every other subscriber too.
package naive

import (
	"sync"

	"tokenstream/internal/stream"
)

// Hub is a blocking fan-out with its own sequence space and source log,
// so its output can be audited by the same verifier as tokenstream's.
type Hub struct {
	mu   sync.Mutex
	seq  uint64
	log  []stream.Event
	subs []stream.FrameSink
}

func NewHub() *Hub { return &Hub{} }

func (h *Hub) Subscribe(s stream.FrameSink) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subs = append(h.subs, s)
}

// Publish assigns the next sequence number, records the event, and
// writes its frame to every subscriber before returning. A slow
// subscriber's WriteFrame blocks the whole fan-out.
func (h *Hub) Publish(ev stream.Event) stream.Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	ev.Seq = h.seq
	h.log = append(h.log, ev)
	f := stream.FrameFor(ev)
	for _, s := range h.subs {
		_ = s.WriteFrame(f)
	}
	return ev
}

// Log returns a copy of the source event log.
func (h *Hub) Log() []stream.Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]stream.Event, len(h.log))
	copy(out, h.log)
	return out
}
