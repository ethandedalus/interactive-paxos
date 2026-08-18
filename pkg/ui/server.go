// Package ui
package ui

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/a-h/templ"

	"github.com/ethandedalus/single-decree-paxos/pkg/events"
	"github.com/ethandedalus/single-decree-paxos/pkg/node"
)

//go:embed static
var static embed.FS

func Static() fs.FS {
	return static
}

type Server struct {
	node *node.Node
	log  *slog.Logger
	http *http.Server
}

func NewServer(addr string, n *node.Node, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}

	s := &Server{node: n, log: log}

	mux := http.NewServeMux()
	mux.Handle("GET /static/", http.FileServerFS(static))
	mux.HandleFunc("GET /", s.handleIndex)
	mux.HandleFunc("GET /api/snapshot", s.handleSnapshot)
	mux.HandleFunc("GET /api/stream", s.handleStream)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("POST /api/kill", s.handleKill)
	mux.HandleFunc("POST /api/revive", s.handleRevive)
	mux.HandleFunc("POST /api/campaign", s.handleCampaign)
	mux.HandleFunc("POST /api/reset", s.handleReset)
	mux.HandleFunc("POST /api/chaos/toggle", s.handleChaosToggle)
	mux.HandleFunc("POST /api/faults/drop", s.handleDrop)
	mux.HandleFunc("POST /api/faults/latency", s.handleLatency)
	mux.HandleFunc("POST /api/faults/isolate", s.handleIsolate)
	mux.HandleFunc("POST /api/faults/peer", s.handlePeer)
	mux.HandleFunc("POST /api/faults/heal", s.handleHeal)

	s.http = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}

func (s *Server) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.http.Shutdown(shutdown)
	}()

	s.log.Info("ui listening", slog.String("addr", s.http.Addr))

	if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("ui server: %w", err)
	}
	return nil
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := Page(s.node.Snapshot()).Render(r.Context(), w); err != nil {
		s.log.Error("render page", slog.String("error", err.Error()))
	}
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.node.Snapshot())
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	var since uint64
	if raw := r.URL.Query().Get("since"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			http.Error(w, "since must be a number", http.StatusBadRequest)
			return
		}
		since = parsed
	}

	writeJSON(w, map[string]any{
		"node_id": s.node.Config().ID,
		"latest":  s.node.Events().Latest(),
		"events":  s.node.Events().Since(since),
	})
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	logs, unsubscribe := s.node.Events().Subscribe()
	defer unsubscribe()

	for _, e := range s.node.Events().Snapshot() {
		s.sendEvent(w, e)
	}
	s.sendState(w)
	flusher.Flush()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case e, open := <-logs:
			if !open {
				return
			}
			s.sendEvent(w, e)
			flusher.Flush()
		case <-ticker.C:
			s.sendState(w)
			flusher.Flush()
		}
	}
}

func (s *Server) sendState(w http.ResponseWriter) {
	snap := s.node.Snapshot()

	stateHTML, err := renderToString(State(snap))
	if err != nil {
		return
	}
	badgesHTML, err := renderToString(Badges(snap))
	if err != nil {
		return
	}
	controlsHTML, err := renderToString(ControlButtons(snap))
	if err != nil {
		return
	}

	writeSSE(w, "state", stateHTML)
	writeSSE(w, "chrome", map[string]string{"badges": badgesHTML, "controls": controlsHTML})
}

func (s *Server) sendEvent(w http.ResponseWriter, e events.Event) {
	html, err := renderToString(Event(e.Level, e.Time.Format("15:04:05.000"), e.Message, e.FieldString()))
	if err != nil {
		return
	}
	writeSSE(w, "log", html)
}

func (s *Server) handleKill(w http.ResponseWriter, r *http.Request) {
	if err := s.node.Kill(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRevive(w http.ResponseWriter, r *http.Request) {
	if err := s.node.Revive(); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	s.node.TriggerCampaign(false)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleCampaign(w http.ResponseWriter, r *http.Request) {
	s.node.TriggerCampaign(false)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if err := s.node.Reset(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleHeal(w http.ResponseWriter, r *http.Request) {
	s.node.Faults().Heal()
	s.log.Warn("all injected faults cleared")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleChaosToggle(w http.ResponseWriter, r *http.Request) {
	snap := s.node.Snapshot()
	s.node.SetChaos(!snap.Chaos, 3*time.Second, 12*time.Second)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleDrop(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Prepare float64 `json:"prepare"`
		Accept  float64 `json:"accept"`
	}
	if !decode(w, r, &body) {
		return
	}
	s.node.Faults().SetDropRates(body.Prepare, body.Accept)
	s.log.Warn("drop rates changed",
		slog.Float64("prepare", body.Prepare),
		slog.Float64("accept", body.Accept),
	)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleLatency(w http.ResponseWriter, r *http.Request) {
	var body struct {
		MinMS int64 `json:"min_ms"`
		MaxMS int64 `json:"max_ms"`
	}
	if !decode(w, r, &body) {
		return
	}
	s.node.Faults().SetLatency(
		time.Duration(body.MinMS)*time.Millisecond,
		time.Duration(body.MaxMS)*time.Millisecond,
	)
	s.log.Warn("latency injection changed",
		slog.Int64("min_ms", body.MinMS),
		slog.Int64("max_ms", body.MaxMS),
	)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleIsolate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Isolated bool `json:"isolated"`
	}
	if !decode(w, r, &body) {
		return
	}
	s.node.Faults().SetIsolated(body.Isolated)
	s.log.Warn("isolation changed", slog.Bool("isolated", body.Isolated))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePeer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PeerID  int  `json:"peer_id"`
		Blocked bool `json:"blocked"`
	}
	if !decode(w, r, &body) {
		return
	}
	s.node.Faults().SetBlocked(body.PeerID, body.Blocked)
	s.log.Warn("peer link changed",
		slog.Int("peer_id", body.PeerID),
		slog.Bool("blocked", body.Blocked),
	)
	w.WriteHeader(http.StatusNoContent)
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeSSE(w http.ResponseWriter, event string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
}

func renderToString(c templ.Component) (string, error) {
	var buf bytes.Buffer
	if err := c.Render(context.Background(), &buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}
