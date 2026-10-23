package stream

import (
	"fmt"
	"strings"
)

// AuditResult is the verdict of Verify. OK is true only if every check
// passed; Errors names each violation found.
type AuditResult struct {
	OK             bool     `json:"ok"`
	Errors         []string `json:"errors,omitempty"`
	Frames         int      `json:"frames"`
	Resyncs        int      `json:"resyncs"`
	StateEvents    int      `json:"state_events_in_log"`
	StateDelivered int      `json:"state_events_delivered"`
	StateSubsumed  int      `json:"state_events_subsumed_by_resync"`
	FinalTextOK    bool     `json:"final_text_ok"`
	FinalStateOK   bool     `json:"final_state_ok"`
}

// stateAt computes the expected state as of seq from an initial state and
// a source log — the same computation the server performs, redone here so
// the audit shares no state with the serving path.
func stateAt(initial map[string]StateVal, log []Event, seq uint64) map[string]StateVal {
	out := make(map[string]StateVal, len(initial))
	for k, v := range initial {
		out[k] = v
	}
	for _, ev := range log {
		if ev.Seq > seq {
			break
		}
		if ev.Kind == KindState {
			out[ev.Key] = StateVal{Value: ev.Value, Version: ev.Version}
		}
	}
	return out
}

func snapshotConsistent(initial map[string]StateVal, log []Event, f Frame) error {
	want := stateAt(initial, log, f.Seq)
	if len(f.Snapshot) != len(want) {
		return fmt.Errorf("frame seq %d (%s): snapshot has %d keys, expected %d", f.Seq, f.Type, len(f.Snapshot), len(want))
	}
	for k, wv := range want {
		gv, ok := f.Snapshot[k]
		if !ok {
			return fmt.Errorf("frame seq %d (%s): snapshot missing key %q", f.Seq, f.Type, k)
		}
		if gv != wv {
			return fmt.Errorf("frame seq %d (%s): snapshot key %q = %+v, expected %+v", f.Seq, f.Type, k, gv, wv)
		}
	}
	return nil
}

func statesEqual(a, b map[string]StateVal) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || bv != av {
			return false
		}
	}
	return true
}

// Verify checks one client's received frames against the session's source
// log. The contract it enforces:
//
//   - frame seqs strictly increase and, if a done frame arrived, end at
//     the last log seq;
//   - the reconstructed text (hello text + token and resync texts, in
//     order) is byte-identical to the source text;
//   - every hello/resync snapshot is exactly the state as of its seq;
//   - the client's final known state equals the source final state;
//   - no silent loss: every state event in the log was either delivered
//     as a state frame or subsumed by a resync (or the hello) whose seq
//     is >= the event's seq and whose snapshot provably reflects it.
//
// frames must be in arrival order and include the hello frame if one was
// sent. A client of the naive fan-out simply has no hello and no resyncs:
// the same checks apply, and any dropped state frame fails the audit.
func Verify(log []Event, initial map[string]StateVal, frames []Frame) AuditResult {
	res := AuditResult{Frames: len(frames)}
	var errs []string

	// Source ground truth.
	var srcText strings.Builder
	lastSeq := uint64(0)
	stateSeqs := make(map[uint64]Event)
	for _, ev := range log {
		if ev.Seq > lastSeq {
			lastSeq = ev.Seq
		}
		switch ev.Kind {
		case KindToken:
			srcText.WriteString(ev.Text)
		case KindState:
			stateSeqs[ev.Seq] = ev
		}
	}
	res.StateEvents = len(stateSeqs)

	var gotText strings.Builder
	known := make(map[string]StateVal, len(initial))
	for k, v := range initial {
		known[k] = v
	}
	delivered := make(map[uint64]bool)
	var resyncSeqs []uint64
	helloSeq := uint64(0)
	haveHello := false
	prevSeq := uint64(0)
	sawDone := false

	for i, f := range frames {
		switch f.Type {
		case "hello":
			if i != 0 {
				errs = append(errs, fmt.Sprintf("hello frame at position %d, expected first", i))
			}
			haveHello = true
			helloSeq = f.Seq
			prevSeq = f.Seq
			if err := snapshotConsistent(initial, log, f); err != nil {
				errs = append(errs, err.Error())
			}
			gotText.WriteString(f.Text)
			known = stateAt(initial, log, f.Seq)
		case "token":
			if f.Seq <= prevSeq {
				errs = append(errs, fmt.Sprintf("token frame seq %d not after %d", f.Seq, prevSeq))
			}
			prevSeq = f.Seq
			gotText.WriteString(f.Text)
		case "state":
			if f.Seq <= prevSeq {
				errs = append(errs, fmt.Sprintf("state frame seq %d not after %d", f.Seq, prevSeq))
			}
			prevSeq = f.Seq
			ev, ok := stateSeqs[f.Seq]
			if !ok {
				errs = append(errs, fmt.Sprintf("state frame seq %d has no matching log event", f.Seq))
			} else if ev.Key != f.Key || ev.Value != f.Value || ev.Version != f.Version {
				errs = append(errs, fmt.Sprintf("state frame seq %d = %s=%q v%d, log says %s=%q v%d", f.Seq, f.Key, f.Value, f.Version, ev.Key, ev.Value, ev.Version))
			}
			delivered[f.Seq] = true
			known[f.Key] = StateVal{Value: f.Value, Version: f.Version}
		case "resync":
			if f.Seq <= prevSeq {
				errs = append(errs, fmt.Sprintf("resync frame seq %d not after %d", f.Seq, prevSeq))
			}
			prevSeq = f.Seq
			res.Resyncs++
			resyncSeqs = append(resyncSeqs, f.Seq)
			if err := snapshotConsistent(initial, log, f); err != nil {
				errs = append(errs, err.Error())
			}
			gotText.WriteString(f.Text)
			known = stateAt(initial, log, f.Seq)
		case "done":
			if f.Seq <= prevSeq {
				errs = append(errs, fmt.Sprintf("done frame seq %d not after %d", f.Seq, prevSeq))
			}
			prevSeq = f.Seq
			sawDone = true
		default:
			errs = append(errs, fmt.Sprintf("unknown frame type %q", f.Type))
		}
	}

	if sawDone && prevSeq != lastSeq {
		errs = append(errs, fmt.Sprintf("stream ended at seq %d, log ends at %d", prevSeq, lastSeq))
	}

	res.FinalTextOK = gotText.String() == srcText.String()
	if !res.FinalTextOK {
		errs = append(errs, fmt.Sprintf("final text mismatch: got %d bytes, source %d bytes", gotText.Len(), srcText.Len()))
	}

	wantFinal := stateAt(initial, log, lastSeq)
	res.FinalStateOK = statesEqual(known, wantFinal)
	if !res.FinalStateOK {
		errs = append(errs, "final state mismatch")
	}

	// No silent loss.
	for seq := range stateSeqs {
		if delivered[seq] {
			res.StateDelivered++
			continue
		}
		covered := haveHello && seq <= helloSeq
		if !covered {
			for _, rs := range resyncSeqs {
				if rs >= seq {
					covered = true
					break
				}
			}
		}
		if covered {
			res.StateSubsumed++
		} else {
			errs = append(errs, fmt.Sprintf("state event seq %d (%s) silently lost: not delivered, no covering resync", seq, stateSeqs[seq].Key))
		}
	}

	res.Errors = errs
	res.OK = len(errs) == 0
	return res
}
