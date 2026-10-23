package server

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tokenstream/internal/stream"
)

// TestEndToEndSSE drives the real HTTP path: create a session, connect
// to its SSE stream, and require the frames to reconstruct the session
// text exactly, starting with a hello frame.
func TestEndToEndSSE(t *testing.T) {
	hub := stream.NewHub(16)
	ts := httptest.NewServer(New(hub).Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/sessions", "application/json", strings.NewReader(`{"prompt":"demo","tokens":30,"rate":2000}`))
	if err != nil {
		t.Fatal(err)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil || created.ID == "" {
		t.Fatalf("create session: id=%q err=%v", created.ID, err)
	}
	resp.Body.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/sessions/"+created.ID+"/stream", nil)
	ctx, cancel := context.WithTimeout(req.Context(), 15*time.Second)
	defer cancel()
	sresp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer sresp.Body.Close()
	if ct := sresp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content type = %q", ct)
	}

	var got strings.Builder
	var types []string
	sc := bufio.NewScanner(sresp.Body)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	var data strings.Builder
	flush := func() bool {
		if data.Len() == 0 {
			return false
		}
		var f stream.Frame
		if err := json.Unmarshal([]byte(data.String()), &f); err == nil {
			types = append(types, f.Type)
			got.WriteString(f.Text)
			if f.Type == "done" {
				return true
			}
		}
		data.Reset()
		return false
	}
	doneSeen := false
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			data.WriteString(strings.TrimPrefix(line, "data: "))
		} else if line == "" {
			if flush() {
				doneSeen = true
				break
			}
		}
	}
	if !doneSeen {
		t.Fatalf("never saw done frame; types=%v", types)
	}
	if len(types) == 0 || types[0] != "hello" {
		t.Fatalf("first frame should be hello, types=%v", types)
	}

	sess := hub.Session(created.ID)
	if sess == nil {
		t.Fatal("session missing")
	}
	if got.String() != sess.Text() {
		t.Fatalf("streamed text %q != session text %q", got.String(), sess.Text())
	}

	// Dashboard serves and wires an EventSource.
	dresp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer dresp.Body.Close()
	body := make([]byte, 1<<20)
	n, _ := dresp.Body.Read(body)
	if !strings.Contains(string(body[:n]), "EventSource") {
		t.Fatal("dashboard does not use EventSource")
	}

	// Store write + snapshot endpoints agree.
	wresp, err := http.Post(ts.URL+"/store", "application/json", strings.NewReader(`{"key":"status","value":"critical"}`))
	if err != nil || wresp.StatusCode != http.StatusOK {
		t.Fatalf("store write: %v %v", wresp.StatusCode, err)
	}
	wresp.Body.Close()
}
