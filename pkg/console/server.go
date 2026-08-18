package console

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/a-h/templ"

	"github.com/ethandedalus/single-decree-paxos/pkg/ui"
)

//go:embed static
var static embed.FS

type runState struct {
	mu       sync.Mutex
	scenario string
	running  bool
	started  time.Time
	steps    []Step
	failed   bool
	cancel   context.CancelFunc
	subs     map[int]chan Step
	nextSub  int

	logSubs    map[int]chan NodeLog
	nextLogSub int
}

type Server struct {
	cluster *Cluster
	log     *slog.Logger
	http    *http.Server
	state   *runState
	tailer  *tailer
}

func NewServer(addr string, cluster *Cluster, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}

	s := &Server{
		cluster: cluster,
		log:     log,
		state: &runState{
			subs:    make(map[int]chan Step),
			logSubs: make(map[int]chan NodeLog),
		},
	}
	s.tailer = newTailer(cluster, 500*time.Millisecond, s.emitNodeLog)

	mux := http.NewServeMux()
	mux.Handle("GET /static/console.js", http.FileServerFS(static))
	mux.Handle("GET /static/", http.FileServerFS(ui.Static()))
	mux.HandleFunc("GET /", s.handleIndex)
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("GET /api/stream", s.handleStream)
	mux.HandleFunc("POST /api/scenario/run", s.handleRunScenario)
	mux.HandleFunc("POST /api/scenario/stop", s.handleStopScenario)
	mux.HandleFunc("POST /api/cluster/{action}", s.handleClusterAction)

	s.http = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return s
}

func (s *Server) Run(ctx context.Context) error {
	go s.tailer.run(ctx)

	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.http.Shutdown(shutdown)
	}()

	s.log.Info("console listening",
		slog.String("addr", s.http.Addr),
		slog.Int("nodes", s.cluster.Size()),
		slog.Int("quorum", s.cluster.Quorum()),
	)

	if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("console server: %w", err)
	}
	return nil
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	status := s.cluster.Status(r.Context())
	if err := ConsolePage(s.cluster, status, Scenarios()).Render(r.Context(), w); err != nil {
		s.log.Error("render console", slog.String("error", err.Error()))
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.cluster.Status(r.Context()))
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

	steps, unsubscribe := s.subscribe()
	defer unsubscribe()

	logs, unsubscribeLogs := s.subscribeLogs()
	defer unsubscribeLogs()

	for _, step := range s.snapshotSteps() {
		s.sendStep(w, step)
	}
	s.sendGrid(w, r.Context())
	flusher.Flush()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case step, open := <-steps:
			if !open {
				return
			}
			s.sendStep(w, step)
			flusher.Flush()
		case entry, open := <-logs:
			if !open {
				return
			}
			s.sendNodeLog(w, entry)
			flusher.Flush()
		case <-ticker.C:
			s.sendGrid(w, r.Context())
			flusher.Flush()
		}
	}
}

func (s *Server) sendNodeLog(w http.ResponseWriter, entry NodeLog) {
	html, err := renderToString(ui.Event(
		entry.Event.Level,
		entry.Event.Time.Format("15:04:05.000"),
		entry.Event.Message,
		entry.Event.FieldString(),
	))
	if err != nil {
		return
	}
	writeSSE(w, "nodelog", map[string]any{"node": entry.NodeID, "html": html})
}

func (s *Server) sendGrid(w http.ResponseWriter, ctx context.Context) {
	status := s.cluster.Status(ctx)

	grid, err := renderToString(NodeGrid(status))
	if err != nil {
		return
	}
	name, running := s.running()
	summary, err := renderToString(Summary(s.cluster, status, name, running))
	if err != nil {
		return
	}

	writeSSE(w, "grid", map[string]string{"grid": grid, "summary": summary})
}

func (s *Server) sendStep(w http.ResponseWriter, step Step) {
	html, err := renderToString(StepRow(step))
	if err != nil {
		return
	}
	writeSSE(w, "step", html)
}

func (s *Server) handleRunScenario(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Scenario string            `json:"scenario"`
		Params   map[string]string `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	scenario, ok := ScenarioByName(body.Scenario)
	if !ok {
		http.Error(w, "unknown scenario "+body.Scenario, http.StatusNotFound)
		return
	}

	if err := s.start(scenario, body.Params); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleStopScenario(w http.ResponseWriter, r *http.Request) {
	s.state.mu.Lock()
	cancel := s.state.cancel
	s.state.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleClusterAction(w http.ResponseWriter, r *http.Request) {
	action := r.PathValue("action")
	ctx := r.Context()

	var err error
	switch action {
	case "reset":
		err = s.cluster.each(ctx, nil, (*Client).Reset)
		if err == nil {
			err = s.cluster.each(ctx, nil, (*Client).Heal)
		}
	case "kill":
		err = s.cluster.each(ctx, nil, (*Client).Kill)
	case "revive":
		err = s.cluster.each(ctx, nil, (*Client).Revive)
	case "campaign":
		err = s.cluster.each(ctx, nil, (*Client).Campaign)
	case "heal":
		err = s.cluster.each(ctx, nil, (*Client).Heal)
		if err == nil {
			err = s.cluster.each(ctx, nil, func(c *Client, ctx context.Context) error {
				return c.SetChaos(ctx, false)
			})
		}
	default:
		http.Error(w, "unknown action "+action, http.StatusNotFound)
		return
	}

	s.emit(Step{At: time.Now(), Kind: StepAction, Message: "cluster action: " + action})

	if err != nil {
		s.emit(Step{At: time.Now(), Kind: StepWarn, Message: err.Error()})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) start(scenario Scenario, params map[string]string) error {
	s.state.mu.Lock()
	if s.state.running {
		s.state.mu.Unlock()
		return errors.New("a scenario is already running")
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.state.running = true
	s.state.failed = false
	s.state.scenario = scenario.Name
	s.state.started = time.Now()
	s.state.steps = nil
	s.state.cancel = cancel
	s.state.mu.Unlock()

	run := &Run{cluster: s.cluster, values: Values(params), emit: s.emit}
	run.Info("scenario %q started against %d nodes, quorum %d", scenario.Title, s.cluster.Size(), s.cluster.Quorum())

	go func() {
		defer cancel()

		err := scenario.Run(ctx, run)

		s.state.mu.Lock()
		s.state.running = false
		s.state.failed = err != nil
		s.state.mu.Unlock()

		switch {
		case err == nil:
			run.Result("scenario %q finished", scenario.Title)
		case errors.Is(err, context.Canceled):
			run.Warn("scenario %q stopped", scenario.Title)
		default:
			run.step(StepFailure, "scenario %q failed: %v", scenario.Title, err)
		}
	}()

	return nil
}

func (s *Server) running() (string, bool) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	return s.state.scenario, s.state.running
}

func (s *Server) emit(step Step) {
	s.state.mu.Lock()
	if step.At.IsZero() {
		step.At = time.Now()
	}
	s.state.steps = append(s.state.steps, step)
	if len(s.state.steps) > 500 {
		s.state.steps = s.state.steps[len(s.state.steps)-500:]
	}
	subs := make([]chan Step, 0, len(s.state.subs))
	for _, ch := range s.state.subs {
		subs = append(subs, ch)
	}
	s.state.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- step:
		default:
		}
	}
}

func (s *Server) emitNodeLog(entry NodeLog) {
	s.state.mu.Lock()
	subs := make([]chan NodeLog, 0, len(s.state.logSubs))
	for _, ch := range s.state.logSubs {
		subs = append(subs, ch)
	}
	s.state.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- entry:
		default:
		}
	}
}

func (s *Server) subscribeLogs() (<-chan NodeLog, func()) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	id := s.state.nextLogSub
	s.state.nextLogSub++
	ch := make(chan NodeLog, 512)
	s.state.logSubs[id] = ch

	return ch, func() {
		s.state.mu.Lock()
		defer s.state.mu.Unlock()
		if existing, ok := s.state.logSubs[id]; ok {
			delete(s.state.logSubs, id)
			close(existing)
		}
	}
}

func (s *Server) snapshotSteps() []Step {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	return append([]Step(nil), s.state.steps...)
}

func (s *Server) subscribe() (<-chan Step, func()) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	id := s.state.nextSub
	s.state.nextSub++
	ch := make(chan Step, 256)
	s.state.subs[id] = ch

	return ch, func() {
		s.state.mu.Lock()
		defer s.state.mu.Unlock()
		if existing, ok := s.state.subs[id]; ok {
			delete(s.state.subs, id)
			close(existing)
		}
	}
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
