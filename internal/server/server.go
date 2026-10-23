// Package server exposes tokenstream over HTTP: session creation, the
// SSE stream, store writes, snapshots, and metrics, plus a static
// dashboard that renders a live dual-source stream in the browser.
package server

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"tokenstream/internal/stream"
)

//go:embed dashboard.html
var dashboardHTML string

type Server struct {
	hub *stream.Hub
	n   atomic.Int64
}

func New(hub *stream.Hub) *Server { return &Server{hub: hub} }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.index)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("POST /sessions", s.createSession)
	mux.HandleFunc("GET /sessions/{id}/stream", s.streamSession)
	mux.HandleFunc("GET /sessions/{id}/snapshot", s.snapshot)
	mux.HandleFunc("POST /store", s.setStore)
	mux.HandleFunc("GET /metrics", s.metrics)
	return mux
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(dashboardHTML))
}

type createReq struct {
	Prompt string  `json:"prompt"`
	Tokens int     `json:"tokens"`
	Rate   float64 `json:"rate"`
}

var stages = []string{"intake", "retrieval", "drafting", "review", "finalizing"}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var req createReq
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Tokens <= 0 || req.Tokens > 5000 {
		req.Tokens = 240
	}
	if req.Rate <= 0 || req.Rate > 5000 {
		req.Rate = 40
	}
	id := fmt.Sprintf("s-%d", s.n.Add(1))
	sess := s.hub.CreateSession(id)
	sess.Prompt = req.Prompt
	tokens := req.Tokens
	go stream.RunGeneration(context.Background(), sess, time.Now().UnixNano(), tokens, req.Rate, func(i int) {
		if i%30 == 29 {
			s.hub.Set("progress", fmt.Sprintf("%d/%d tokens", i+1, tokens))
		}
		if i%80 == 79 {
			s.hub.Set("stage", stages[(i/80)%len(stages)])
		}
	})
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (s *Server) streamSession(w http.ResponseWriter, r *http.Request) {
	sess := s.hub.Session(r.PathValue("id"))
	if sess == nil {
		http.NotFound(w, r)
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	pump, hello := sess.Subscribe()
	defer sess.Unsubscribe(pump)
	rc := http.NewResponseController(w)

	write := func(f stream.Frame) bool {
		_ = rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := w.Write(stream.EncodeFrame(f)); err != nil {
			return false
		}
		fl.Flush()
		pump.Ack(f)
		return true
	}
	if !write(hello) {
		return
	}
	for {
		f, ok := pump.Next(r.Context())
		if !ok {
			return
		}
		if !write(f) {
			return
		}
		if f.Type == "done" {
			return
		}
	}
}

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	sess := s.hub.Session(r.PathValue("id"))
	if sess == nil {
		http.NotFound(w, r)
		return
	}
	seq := sess.CurrentSeq()
	writeJSON(w, http.StatusOK, map[string]any{
		"id":    sess.ID,
		"seq":   seq,
		"text":  sess.Text(),
		"state": sess.StateAsOf(seq),
		"done":  sess.Done(),
	})
}

type setReq struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) setStore(w http.ResponseWriter, r *http.Request) {
	var req setReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
		http.Error(w, "need {key, value}", http.StatusBadRequest)
		return
	}
	v := s.hub.Set(req.Key, req.Value)
	writeJSON(w, http.StatusOK, map[string]any{"key": req.Key, "value": v.Value, "version": v.Version})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	type pumpOut struct {
		Stats stream.Stats `json:"stats"`
	}
	type sessOut struct {
		ID    string    `json:"id"`
		Seq   uint64    `json:"seq"`
		Done  bool      `json:"done"`
		Pumps []pumpOut `json:"pumps"`
	}
	out := []sessOut{}
	for _, sess := range s.hub.Sessions() {
		so := sessOut{ID: sess.ID, Seq: sess.CurrentSeq(), Done: sess.Done()}
		for _, p := range sess.Pumps() {
			so.Pumps = append(so.Pumps, pumpOut{Stats: p.Stats()})
		}
		out = append(out, so)
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": out})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
