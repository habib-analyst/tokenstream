// Package harness runs the measured workload: one deterministic token
// generation plus interleaved and bursted store writes, fanned out to
// fast, slow, and paused clients — once through tokenstream, once
// through a fast-clients-only control, and once through the naive
// blocking fan-out on the identical workload.
package harness

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"tokenstream/internal/naive"
	"tokenstream/internal/stream"
)

// Recorder collects frames with their arrival (write-completion) times.
type Recorder struct {
	mu     sync.Mutex
	frames []stream.ReceivedFrame
}

func (r *Recorder) add(f stream.Frame) {
	r.mu.Lock()
	r.frames = append(r.frames, stream.ReceivedFrame{Frame: f, At: time.Now()})
	r.mu.Unlock()
}

func (r *Recorder) Received() []stream.ReceivedFrame {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]stream.ReceivedFrame, len(r.frames))
	copy(out, r.frames)
	return out
}

func (r *Recorder) Frames() []stream.Frame {
	rec := r.Received()
	out := make([]stream.Frame, len(rec))
	for i, rf := range rec {
		out[i] = rf.Frame
	}
	return out
}

// FastSink records each frame immediately.
type FastSink struct{ Rec *Recorder }

func (s *FastSink) WriteFrame(f stream.Frame) error { s.Rec.add(f); return nil }

// SlowSink models a client reading one byte at a time with a fixed
// per-byte delay: a frame lands only after its full wire size has been
// paced out.
type SlowSink struct {
	Rec     *Recorder
	PerByte time.Duration
}

func (s *SlowSink) WriteFrame(f stream.Frame) error {
	n := len(stream.EncodeFrame(f))
	if s.PerByte > 0 && n > 0 {
		time.Sleep(time.Duration(n) * s.PerByte)
	}
	s.Rec.add(f)
	return nil
}

// PausedSink models a stalled client (frozen tab, dead network): every
// write blocks until the pause instant passes, then it reads at line
// rate.
type PausedSink struct {
	Rec   *Recorder
	Until time.Time
}

func (s *PausedSink) WriteFrame(f stream.Frame) error {
	if d := time.Until(s.Until); d > 0 {
		time.Sleep(d)
	}
	s.Rec.add(f)
	return nil
}

// Workload parameters, identical across scenarios.
type Workload struct {
	Tokens     int
	RatePerSec float64
	Seed       int64
	Burst      int // extra state writes fired concurrently at generation start
}

var keys = []string{"patient", "status", "dose", "ward"}
var burstKeys = []string{"burst-a", "burst-b", "burst-c", "burst-d"}

// driveInterleaved runs hook logic shared by all scenarios: every 25th
// token also writes one rotating key.
func interleave(i int, set func(key, value string)) {
	if i%25 == 24 {
		set(keys[(i/25)%len(keys)], fmt.Sprintf("v%d", i))
	}
}

func burst(set func(key, value string), n int, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := 0; j < n; j++ {
		set(burstKeys[j%len(burstKeys)], fmt.Sprintf("b%d", j))
	}
}

// ClientReport is the measured result for one client in one scenario.
type ClientReport struct {
	Name         string             `json:"name"`
	Class        string             `json:"class"`
	TTFTms       float64            `json:"ttft_ms"`
	TokenP50ms   float64            `json:"token_interarrival_p50_ms"`
	TokenP95ms   float64            `json:"token_interarrival_p95_ms"`
	CompletionMs float64            `json:"completion_ms"`
	Frames       int                `json:"frames"`
	Pump         *stream.Stats      `json:"pump,omitempty"`
	Audit        stream.AuditResult `json:"audit"`
}

// ScenarioReport is one full workload run.
type ScenarioReport struct {
	Scenario         string         `json:"scenario"`
	PublisherMs      float64        `json:"publisher_ms"`
	PeakPendingBytes int            `json:"peak_pending_bytes,omitempty"`
	PeakLagEvents    uint64         `json:"peak_lag_events,omitempty"`
	HeapBeforeBytes  uint64         `json:"heap_before_bytes,omitempty"`
	HeapAfterBytes   uint64         `json:"heap_after_bytes,omitempty"`
	RSSBeforeBytes   int64          `json:"rss_before_bytes,omitempty"`
	RSSAfterBytes    int64          `json:"rss_after_bytes,omitempty"`
	Clients          []ClientReport `json:"clients"`
}

// Results is the full experiment output (results.json).
type Results struct {
	Workload  Workload         `json:"workload"`
	LagBudget int              `json:"lag_budget_state_events"`
	SlowByte  string           `json:"slow_client_pacing"`
	PauseFor  string           `json:"paused_client_pause"`
	Scenarios []ScenarioReport `json:"scenarios"`
}

func percentile(durs []time.Duration, q float64) float64 {
	if len(durs) == 0 {
		return 0
	}
	s := make([]time.Duration, len(durs))
	copy(s, durs)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(q * float64(len(s)-1))
	return float64(s[idx]) / float64(time.Millisecond)
}

func summarize(name, class string, rec *Recorder, t0 time.Time, log []stream.Event, initial map[string]stream.StateVal) ClientReport {
	frames := rec.Received()
	rep := ClientReport{Name: name, Class: class, Frames: len(frames)}
	if len(frames) == 0 {
		rep.Audit = stream.AuditResult{OK: false, Errors: []string{"no frames received"}}
		return rep
	}
	// TTFT is time to first generated content: the first token or
	// resync frame. (The hello frame precedes generation by
	// construction and is not content.)
	rep.TTFTms = float64(frames[0].At.Sub(t0)) / float64(time.Millisecond)
	for _, f := range frames {
		if f.Frame.Type == "token" || f.Frame.Type == "resync" {
			rep.TTFTms = float64(f.At.Sub(t0)) / float64(time.Millisecond)
			break
		}
	}
	var gaps []time.Duration
	var prev *stream.ReceivedFrame
	for i := range frames {
		f := frames[i]
		if f.Frame.Type == "token" || f.Frame.Type == "resync" {
			if prev != nil {
				gaps = append(gaps, f.At.Sub(prev.At))
			}
			cp := f
			prev = &cp
		}
		if f.Frame.Type == "done" {
			rep.CompletionMs = float64(f.At.Sub(t0)) / float64(time.Millisecond)
		}
	}
	if rep.CompletionMs == 0 {
		rep.CompletionMs = float64(frames[len(frames)-1].At.Sub(t0)) / float64(time.Millisecond)
	}
	rep.TokenP50ms = percentile(gaps, 0.50)
	rep.TokenP95ms = percentile(gaps, 0.95)
	rep.Audit = stream.Verify(log, initial, rec.Frames())
	return rep
}

func rssBytes() int64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * int64(os.Getpagesize())
}

func heapBytes() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// runTokenstream executes one tokenstream scenario. clients are built by
// the caller via the add func so the scenario can attach different sink
// classes while sharing the plumbing.
func runTokenstream(wl Workload, budget int, withSlow bool) ScenarioReport {
	hub := stream.NewHub(budget)
	// Pre-session store state: the sessions' initial snapshot.
	hub.Store().Set("patient", "admitted")
	hub.Store().Set("status", "stable")
	initial := hub.Store().Snapshot()
	sess := hub.CreateSession("exp")
	sess.Prompt = "experiment"

	type client struct {
		name, class string
		rec         *Recorder
		pump        *stream.Pump
		done        chan struct{}
	}
	var clients []*client
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	add := func(name, class string, sink stream.FrameSink, rec *Recorder) {
		pump, hello := sess.Subscribe()
		rec.add(hello)
		c := &client{name: name, class: class, rec: rec, pump: pump, done: make(chan struct{})}
		clients = append(clients, c)
		go func() {
			defer close(c.done)
			for {
				f, ok := pump.Next(ctx)
				if !ok {
					return
				}
				_ = sink.WriteFrame(f)
				pump.Ack(f)
				if f.Type == "done" {
					return
				}
			}
		}()
	}

	recF1, recF2 := &Recorder{}, &Recorder{}
	add("fast-1", "fast", &FastSink{Rec: recF1}, recF1)
	add("fast-2", "fast", &FastSink{Rec: recF2}, recF2)
	if withSlow {
		recS := &Recorder{}
		add("slow-1", "slow", &SlowSink{Rec: recS, PerByte: 60 * time.Microsecond}, recS)
		recP := &Recorder{}
		add("paused-1", "paused", &PausedSink{Rec: recP, Until: time.Now().Add(1500 * time.Millisecond)}, recP)
	}

	// Peak-pending monitor.
	peakBytes := 0
	peakLag := uint64(0)
	stopMon := make(chan struct{})
	monStopped := make(chan struct{})
	go func() {
		defer close(monStopped)
		t := time.NewTicker(2 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopMon:
				return
			case <-t.C:
				sum := 0
				for _, c := range clients {
					st := c.pump.Stats()
					sum += st.PendingBytes
					if st.LagEvents > peakLag {
						peakLag = st.LagEvents
					}
				}
				if sum > peakBytes {
					peakBytes = sum
				}
			}
		}
	}()

	heapBefore := heapBytes()
	rssBefore := rssBytes()

	t0 := time.Now()
	var bwg sync.WaitGroup
	bwg.Add(1)
	go burst(func(k, v string) { hub.Set(k, v) }, wl.Burst, &bwg)
	stream.RunGeneration(ctx, sess, wl.Seed, wl.Tokens, wl.RatePerSec, func(i int) {
		interleave(i, func(k, v string) { hub.Set(k, v) })
	})
	bwg.Wait()
	publisherMs := float64(time.Since(t0)) / float64(time.Millisecond)

	for _, c := range clients {
		select {
		case <-c.done:
		case <-time.After(60 * time.Second):
		}
	}
	close(stopMon)
	<-monStopped

	rep := ScenarioReport{
		Scenario:         "tokenstream-fast-only",
		PublisherMs:      publisherMs,
		PeakPendingBytes: peakBytes,
		PeakLagEvents:    peakLag,
		HeapBeforeBytes:  heapBefore,
		HeapAfterBytes:   heapBytes(),
		RSSBeforeBytes:   rssBefore,
		RSSAfterBytes:    rssBytes(),
	}
	if withSlow {
		rep.Scenario = "tokenstream-mixed"
	}
	log := sess.Log()
	for _, c := range clients {
		cr := summarize(c.name, c.class, c.rec, t0, log, initial)
		st := c.pump.Stats()
		cr.Pump = &st
		rep.Clients = append(rep.Clients, cr)
		sess.Unsubscribe(c.pump)
	}
	return rep
}

// runNaive executes the identical workload through the blocking fan-out.
// The generator loop itself is the publisher, so its total duration is
// the publisher-blocking measure.
func runNaive(wl Workload) ScenarioReport {
	nh := naive.NewHub()
	initial := map[string]stream.StateVal{
		"patient": {Value: "admitted", Version: 1},
		"status":  {Value: "stable", Version: 1},
	}
	recF1, recF2, recS := &Recorder{}, &Recorder{}, &Recorder{}
	nh.Subscribe(&FastSink{Rec: recF1})
	nh.Subscribe(&FastSink{Rec: recF2})
	nh.Subscribe(&SlowSink{Rec: recS, PerByte: 60 * time.Microsecond})

	versions := make(map[string]uint64)
	for k := range initial {
		versions[k] = 1
	}
	var vmu sync.Mutex
	set := func(k, v string) {
		vmu.Lock()
		versions[k]++
		ver := versions[k]
		vmu.Unlock()
		nh.Publish(stream.Event{Kind: stream.KindState, Key: k, Value: v, Version: ver})
	}

	t0 := time.Now()
	var bwg sync.WaitGroup
	bwg.Add(1)
	go burst(set, wl.Burst, &bwg)

	// Same deterministic token stream as the tokenstream scenarios (same
	// seed, same vocab path): regenerate it via a throwaway session so
	// the texts are identical by construction.
	tmp := stream.NewHub(1).CreateSession("tmp")
	stream.RunGeneration(context.Background(), tmp, wl.Seed, wl.Tokens, 1e9, nil)
	tokIdx := 0
	for _, ev := range tmp.Log() {
		if ev.Kind != stream.KindToken {
			continue
		}
		nh.Publish(stream.Event{Kind: stream.KindToken, Text: ev.Text})
		interleave(tokIdx, set)
		tokIdx++
	}
	bwg.Wait()
	nh.Publish(stream.Event{Kind: stream.KindDone})
	publisherMs := float64(time.Since(t0)) / float64(time.Millisecond)

	rep := ScenarioReport{Scenario: "naive-mixed", PublisherMs: publisherMs}
	log := nh.Log()
	rep.Clients = append(rep.Clients,
		summarize("fast-1", "fast", recF1, t0, log, initial),
		summarize("fast-2", "fast", recF2, t0, log, initial),
		summarize("slow-1", "slow", recS, t0, log, initial),
	)
	return rep
}

// RunAll executes the control, mixed, and naive scenarios.
func RunAll() Results {
	wl := Workload{Tokens: 300, RatePerSec: 2000, Seed: 42, Burst: 240}
	budget := 48
	control := runTokenstream(wl, budget, false)
	mixed := runTokenstream(wl, budget, true)
	nv := runNaive(wl)
	return Results{
		Workload:  wl,
		LagBudget: budget,
		SlowByte:  "1 byte per 60us (~16.6 KB/s)",
		PauseFor:  "1500ms",
		Scenarios: []ScenarioReport{control, mixed, nv},
	}
}
