package stream

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func drainClosed(t *testing.T, p *Pump) []Frame {
	t.Helper()
	p.Close()
	ctx := context.Background()
	var out []Frame
	for {
		f, ok := p.Next(ctx)
		if !ok {
			return out
		}
		out = append(out, f)
		p.Ack(f)
	}
}

func fixedSnap(map[string]StateVal) func(uint64) map[string]StateVal {
	return func(uint64) map[string]StateVal { return map[string]StateVal{"k": {Value: "latest", Version: 10}} }
}

func TestCoalescingPreservesFinalText(t *testing.T) {
	p := NewPump(48, fixedSnap(nil))
	var want strings.Builder
	for i := 0; i < 100; i++ {
		tok := fmt.Sprintf("t%d ", i)
		want.WriteString(tok)
		p.Offer(Event{Seq: uint64(i + 1), Kind: KindToken, Text: tok})
	}
	frames := drainClosed(t, p)
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 coalesced frame, got %d", len(frames))
	}
	f := frames[0]
	if f.Type != "token" || f.Text != want.String() || f.Seq != 100 {
		t.Fatalf("bad coalesced frame: type=%s seq=%d text=%q", f.Type, f.Seq, f.Text)
	}
	st := p.Stats()
	if st.Coalesced != 99 {
		t.Fatalf("coalesced = %d, want 99", st.Coalesced)
	}
}

func TestStateBudgetTriggersResync(t *testing.T) {
	p := NewPump(4, fixedSnap(nil))
	for i := 1; i <= 10; i++ {
		p.Offer(Event{Seq: uint64(i), Kind: KindState, Key: "k", Value: fmt.Sprint(i), Version: uint64(i)})
	}
	frames := drainClosed(t, p)
	if len(frames) != 1 || frames[0].Type != "resync" {
		t.Fatalf("expected a single resync frame, got %+v", frames)
	}
	f := frames[0]
	if f.Seq != 10 {
		t.Fatalf("resync seq = %d, want 10", f.Seq)
	}
	if f.Snapshot["k"].Value != "latest" || f.Snapshot["k"].Version != 10 {
		t.Fatalf("resync snapshot wrong: %+v", f.Snapshot)
	}
	st := p.Stats()
	if st.Resyncs != 2 {
		t.Fatalf("resyncs = %d, want 2 (breach at 5 and at 10)", st.Resyncs)
	}
	if st.DroppedState != 10 {
		t.Fatalf("subsumed state = %d, want 10", st.DroppedState)
	}
}

func TestResyncCarriesPendingTokenText(t *testing.T) {
	p := NewPump(4, fixedSnap(nil))
	seq := uint64(0)
	offer := func(ev Event) { seq++; ev.Seq = seq; p.Offer(ev) }
	for _, tok := range []string{"a ", "b ", "c ", "d ", "e "} {
		offer(Event{Kind: KindToken, Text: tok})
	}
	for i := 0; i < 10; i++ {
		offer(Event{Kind: KindState, Key: "k", Value: fmt.Sprint(i), Version: uint64(i)})
	}
	for _, tok := range []string{"f ", "g ", "h "} {
		offer(Event{Kind: KindToken, Text: tok})
	}
	frames := drainClosed(t, p)
	if len(frames) != 2 {
		t.Fatalf("expected resync + token frames, got %+v", frames)
	}
	if frames[0].Type != "resync" || frames[0].Text != "a b c d e " {
		t.Fatalf("resync frame wrong: %+v", frames[0])
	}
	if frames[1].Type != "token" || frames[1].Text != "f g h " {
		t.Fatalf("token frame wrong: %+v", frames[1])
	}
	if !(frames[0].Seq < frames[1].Seq) {
		t.Fatalf("frame seqs not increasing: %d then %d", frames[0].Seq, frames[1].Seq)
	}
}

type collectSink struct {
	mu     sync.Mutex
	frames []Frame
}

func (c *collectSink) add(f Frame) {
	c.mu.Lock()
	c.frames = append(c.frames, f)
	c.mu.Unlock()
}

func (c *collectSink) all() []Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Frame, len(c.frames))
	copy(out, c.frames)
	return out
}

type gatedSink struct {
	collectSink
	gate chan struct{}
	once sync.Once
}

func (g *gatedSink) WriteFrame(f Frame) error {
	g.once.Do(func() { <-g.gate })
	g.add(f)
	return nil
}

func runWriter(ctx context.Context, p *Pump, sink FrameSink, done chan<- struct{}) {
	for {
		f, ok := p.Next(ctx)
		if !ok {
			break
		}
		_ = sink.WriteFrame(f)
		p.Ack(f)
		if f.Type == "done" {
			break
		}
	}
	close(done)
}

// TestAuditFastAndStalledClients runs a full session: a fast client and
// a client whose first write is gated shut while the whole workload is
// published. The publisher must not block, and both clients' streams
// must pass the audit — the stalled one via resync.
func TestAuditFastAndStalledClients(t *testing.T) {
	hub := NewHub(8)
	hub.Store().Set("patient", "admitted")
	sess := hub.CreateSession("t")
	initial := sess.Initial()

	pumpFast, helloFast := sess.Subscribe()
	pumpSlow, helloSlow := sess.Subscribe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fastSink := &collectSink{}
	fastSink.add(helloFast)
	slowSink := &gatedSink{gate: make(chan struct{})}
	slowSink.add(helloSlow)
	fastDone := make(chan struct{})
	slowDone := make(chan struct{})
	go runWriter(ctx, pumpFast, &fastAdapter{fastSink}, fastDone)
	go runWriter(ctx, pumpSlow, slowSink, slowDone)

	start := time.Now()
	for i := 0; i < 60; i++ {
		sess.Publish(Event{Kind: KindToken, Text: fmt.Sprintf("w%d ", i)})
		if i%10 == 9 {
			hub.Set("status", fmt.Sprintf("s%d", i))
		}
	}
	for j := 0; j < 100; j++ {
		hub.Set("burst", fmt.Sprintf("b%d", j))
	}
	sess.Publish(Event{Kind: KindDone})
	publishElapsed := time.Since(start)
	if publishElapsed > 2*time.Second {
		t.Fatalf("publisher blocked on stalled client: %v", publishElapsed)
	}

	close(slowSink.gate)
	for name, ch := range map[string]chan struct{}{"fast": fastDone, "slow": slowDone} {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s client did not finish", name)
		}
	}

	log := sess.Log()
	fastRes := Verify(log, initial, fastSink.all())
	if !fastRes.OK {
		t.Fatalf("fast client audit failed: %v", fastRes.Errors)
	}
	slowRes := Verify(log, initial, slowSink.all())
	if !slowRes.OK {
		t.Fatalf("stalled client audit failed: %v", slowRes.Errors)
	}
	if slowRes.Resyncs < 1 {
		t.Fatalf("stalled client should have resynced at least once, got %d", slowRes.Resyncs)
	}
	if !slowRes.FinalTextOK || !slowRes.FinalStateOK {
		t.Fatalf("stalled client final text/state wrong: %+v", slowRes)
	}
}

// fastAdapter adapts collectSink (which records via add) to FrameSink.
type fastAdapter struct{ c *collectSink }

func (a *fastAdapter) WriteFrame(f Frame) error { a.c.add(f); return nil }

// TestAuditDetectsSilentLoss proves the audit is not vacuous: removing
// one state frame (with no resync to cover it) must fail verification.
func TestAuditDetectsSilentLoss(t *testing.T) {
	hub := NewHub(64)
	sess := hub.CreateSession("t")
	initial := sess.Initial()
	pump, hello := sess.Subscribe()
	sink := &collectSink{}
	sink.add(hello)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go runWriter(ctx, pump, &fastAdapter{sink}, done)

	sess.Publish(Event{Kind: KindToken, Text: "hello "})
	hub.Set("status", "stable")
	sess.Publish(Event{Kind: KindToken, Text: "world"})
	hub.Set("status", "critical")
	sess.Publish(Event{Kind: KindDone})
	<-done

	log := sess.Log()
	frames := sink.all()
	if res := Verify(log, initial, frames); !res.OK {
		t.Fatalf("baseline audit should pass: %v", res.Errors)
	}

	// Drop the first state frame: silent loss.
	var dropped []Frame
	removed := false
	for _, f := range frames {
		if f.Type == "state" && !removed {
			removed = true
			continue
		}
		dropped = append(dropped, f)
	}
	if !removed {
		t.Fatal("no state frame to drop")
	}
	res := Verify(log, initial, dropped)
	if res.OK {
		t.Fatal("audit passed despite a silently dropped state frame")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e, "silently lost") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a silent-loss error, got %v", res.Errors)
	}

	// Tamper with a state value: must also fail.
	var tampered []Frame
	for _, f := range frames {
		if f.Type == "state" {
			f.Value = "forged"
		}
		tampered = append(tampered, f)
	}
	if res := Verify(log, initial, tampered); res.OK {
		t.Fatal("audit passed despite a forged state value")
	}
}

// TestSequenceMonotonicUnderConcurrentPublishers hammers one pump from
// several goroutines through the session (which serializes seqs and
// offers) and requires strictly increasing frame seqs.
func TestSequenceMonotonicUnderConcurrentPublishers(t *testing.T) {
	hub := NewHub(16)
	sess := hub.CreateSession("t")
	pump, _ := sess.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &collectSink{}
	done := make(chan struct{})
	go runWriter(ctx, pump, &fastAdapter{sink}, done)

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if i%7 == 3 {
					hub.Set(fmt.Sprintf("k%d", g), fmt.Sprintf("v%d", i))
				} else {
					sess.Publish(Event{Kind: KindToken, Text: "x "})
				}
			}
		}(g)
	}
	wg.Wait()
	sess.Publish(Event{Kind: KindDone})
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("writer did not finish")
	}
	frames := sink.all()
	for i := 1; i < len(frames); i++ {
		if frames[i].Seq <= frames[i-1].Seq {
			t.Fatalf("seq not increasing at %d: %d after %d", i, frames[i].Seq, frames[i-1].Seq)
		}
	}
	last := sess.CurrentSeq()
	if frames[len(frames)-1].Seq != last {
		t.Fatalf("last frame seq %d, session seq %d", frames[len(frames)-1].Seq, last)
	}
}
