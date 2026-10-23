package stream

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// Hub owns the store and all sessions. A store write is published as a
// state event on every live session, so one client stream merges the
// token source and the change feed.
type Hub struct {
	mu        sync.Mutex
	store     *Store
	sessions  map[string]*Session
	lagBudget int
	nextID    int
}

// NewHub creates a hub whose pumps use the given lag budget (in pending
// state events) before collapsing to a resync.
func NewHub(lagBudget int) *Hub {
	return &Hub{
		store:     NewStore(),
		sessions:  make(map[string]*Session),
		lagBudget: lagBudget,
	}
}

func (h *Hub) Store() *Store { return h.store }

// Set writes to the store and fans the change out to all live sessions.
// It returns the stored value with its new version.
func (h *Hub) Set(key, value string) StateVal {
	v := h.store.Set(key, value)
	h.mu.Lock()
	sessions := make([]*Session, 0, len(h.sessions))
	for _, s := range h.sessions {
		sessions = append(sessions, s)
	}
	h.mu.Unlock()
	for _, s := range sessions {
		if !s.Done() {
			s.Publish(Event{Kind: KindState, Key: key, Value: v.Value, Version: v.Version})
		}
	}
	return v
}

// CreateSession registers a session. The store state at this moment is
// the session's initial state: later snapshots are computed from it plus
// the session log, so a snapshot stamped with seq S reflects exactly the
// events up to S — never a later write that merely raced ahead in the
// store.
func (h *Hub) CreateSession(id string) *Session {
	h.mu.Lock()
	defer h.mu.Unlock()
	if id == "" {
		h.nextID++
		id = "s" + itoa(h.nextID)
	}
	s := &Session{
		ID:      id,
		hub:     h,
		initial: h.store.Snapshot(),
		subs:    make(map[*Pump]struct{}),
	}
	h.sessions[id] = s
	return s
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// Session looks up a session by id.
func (h *Hub) Session(id string) *Session {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sessions[id]
}

// Sessions returns all live sessions.
func (h *Hub) Sessions() []*Session {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*Session, 0, len(h.sessions))
	for _, s := range h.sessions {
		out = append(out, s)
	}
	return out
}

// A Session is one dual-source stream: a token generation plus every
// store change made while it is live. The session keeps the full source
// log — the ground truth the audit verifies clients against.
type Session struct {
	ID     string
	Prompt string

	hub     *Hub
	mu      sync.Mutex
	seq     uint64
	text    strings.Builder
	log     []Event
	initial map[string]StateVal
	subs    map[*Pump]struct{}
	done    bool
}

// Publish assigns the next sequence number, records the event in the
// source log, and offers it to every subscriber. Offers happen under
// the session lock so every pump receives events in strict sequence
// order even with concurrent publishers; Offer is O(1) and never blocks
// on a client, so Publish never blocks on a client either.
func (s *Session) Publish(ev Event) Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	ev.Seq = s.seq
	s.log = append(s.log, ev)
	if ev.Kind == KindToken {
		s.text.WriteString(ev.Text)
	}
	if ev.Kind == KindDone {
		s.done = true
	}
	for p := range s.subs {
		p.Offer(ev)
	}
	return ev
}

// Subscribe registers a new pump and returns it with the hello frame:
// the text so far and the state exactly as of the current sequence, so a
// late joiner starts from a consistent position and only receives newer
// events.
func (s *Session) Subscribe() (*Pump, Frame) {
	p := NewPump(s.hub.lagBudget, s.StateAsOf)
	s.mu.Lock()
	hello := Frame{
		Type:     "hello",
		Seq:      s.seq,
		Text:     s.text.String(),
		Snapshot: s.stateAsOfLocked(s.seq),
	}
	s.subs[p] = struct{}{}
	s.mu.Unlock()
	return p, hello
}

// Unsubscribe detaches a pump and closes it.
func (s *Session) Unsubscribe(p *Pump) {
	s.mu.Lock()
	delete(s.subs, p)
	s.mu.Unlock()
	p.Close()
}

// Pumps returns the currently subscribed pumps (for metrics).
func (s *Session) Pumps() []*Pump {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Pump, 0, len(s.subs))
	for p := range s.subs {
		out = append(out, p)
	}
	return out
}

// Log returns a copy of the source event log.
func (s *Session) Log() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.log))
	copy(out, s.log)
	return out
}

// Text returns the full generated text so far.
func (s *Session) Text() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.text.String()
}

// Done reports whether the generation has finished.
func (s *Session) Done() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// CurrentSeq returns the latest published sequence (0 if none).
func (s *Session) CurrentSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq
}

// Initial returns the store state captured at session creation.
func (s *Session) Initial() map[string]StateVal {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]StateVal, len(s.initial))
	for k, v := range s.initial {
		out[k] = v
	}
	return out
}

// StateAsOf returns the state exactly as of the given sequence: the
// initial state advanced by every state event in the log up to seq.
func (s *Session) StateAsOf(seq uint64) map[string]StateVal {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stateAsOfLocked(seq)
}

func (s *Session) stateAsOfLocked(seq uint64) map[string]StateVal {
	out := make(map[string]StateVal, len(s.initial))
	for k, v := range s.initial {
		out[k] = v
	}
	for _, ev := range s.log {
		if ev.Seq > seq {
			break
		}
		if ev.Kind == KindState {
			out[ev.Key] = StateVal{Value: ev.Value, Version: ev.Version}
		}
	}
	return out
}

var vocab = []string{
	"the ", "quick ", "brown ", "fox ", "jumps ", "over ", "lazy ", "dog ",
	"data ", "stream ", "token ", "model ", "cache ", "query ", "result ",
	"patient ", "scan ", "image ", "report ", "system ", "delta ", "state ",
	"event ", "client ", "server ", "buffer ", "window ", "signal ",
}

// TokenAt returns the i-th token of the deterministic stream for a seed,
// independent of any session — the generator and only the generator uses
// it; ground truth always comes from the session log.
func tokenFrom(rng *rand.Rand) string {
	return vocab[rng.Intn(len(vocab))]
}

// RunGeneration publishes n tokens at the given rate, then a done event.
// hook (may be nil) runs after each token with its index; experiments use
// it to interleave store writes deterministically. The token sequence is
// a pure function of seed.
func RunGeneration(ctx context.Context, s *Session, seed int64, n int, ratePerSec float64, hook func(i int)) {
	rng := rand.New(rand.NewSource(seed))
	if ratePerSec <= 0 {
		ratePerSec = 40
	}
	interval := time.Duration(float64(time.Second) / ratePerSec)
	t := time.NewTicker(interval)
	defer t.Stop()
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s.Publish(Event{Kind: KindToken, Text: tokenFrom(rng)})
		if hook != nil {
			hook(i)
		}
	}
	s.Publish(Event{Kind: KindDone})
}
