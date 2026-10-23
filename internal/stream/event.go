package stream

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Kind classifies a source event. The kind decides the backpressure policy:
// token deltas are coalescible, state changes are not.
type Kind int

const (
	KindToken Kind = iota
	KindState
	KindDone
)

// Event is one source event on a session stream, before fan-out.
// Seq is assigned by the session at publish time (1-based, gapless).
type Event struct {
	Seq     uint64
	Kind    Kind
	Text    string // token delta (KindToken)
	Key     string // state key (KindState)
	Value   string // state value (KindState)
	Version uint64 // per-key version (KindState)
}

// StateVal is one key's value plus its per-key version.
type StateVal struct {
	Value   string `json:"value"`
	Version uint64 `json:"version"`
}

// Frame is one SSE message delivered to a client. Seq is the stream
// position the frame advances the client to: for a token frame it is the
// seq of the newest delta included, for a state frame the event's seq,
// for a resync the seq its snapshot is valid at. Frame seqs on one
// connection are strictly increasing, so any gap a client cannot account
// for is detectable.
type Frame struct {
	Type     string              `json:"type"` // hello|token|state|resync|done
	Seq      uint64              `json:"seq"`
	Text     string              `json:"text,omitempty"`
	Key      string              `json:"key,omitempty"`
	Value    string              `json:"value,omitempty"`
	Version  uint64              `json:"version,omitempty"`
	Snapshot map[string]StateVal `json:"snapshot,omitempty"`
	Reason   string              `json:"reason,omitempty"`
}

// FrameFor converts a single source event to its (uncoalesced) frame.
func FrameFor(ev Event) Frame {
	switch ev.Kind {
	case KindToken:
		return Frame{Type: "token", Seq: ev.Seq, Text: ev.Text}
	case KindState:
		return Frame{Type: "state", Seq: ev.Seq, Key: ev.Key, Value: ev.Value, Version: ev.Version}
	case KindDone:
		return Frame{Type: "done", Seq: ev.Seq}
	}
	return Frame{}
}

// EncodeFrame renders a frame as an SSE message.
func EncodeFrame(f Frame) []byte {
	data, _ := json.Marshal(f)
	var b strings.Builder
	b.WriteString("event: ")
	b.WriteString(f.Type)
	b.WriteString("\n")
	fmt.Fprintf(&b, "id: %d\n", f.Seq)
	b.WriteString("data: ")
	b.Write(data)
	b.WriteString("\n\n")
	return []byte(b.String())
}

// ReceivedFrame is a frame plus the time a client finished receiving it.
type ReceivedFrame struct {
	Frame Frame
	At    time.Time
}

// FrameSink is anything a frame can be written to: an HTTP response in
// production, an instrumented recorder in the harness.
type FrameSink interface {
	WriteFrame(Frame) error
}
