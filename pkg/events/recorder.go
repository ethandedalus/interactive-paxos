// Package events
package events

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

type Field struct {
	Key   string
	Value string
}

type Event struct {
	Seq     uint64
	Time    time.Time
	Level   string
	Message string
	Fields  []Field
}

func (e Event) FieldString() string {
	parts := make([]string, 0, len(e.Fields))
	for _, f := range e.Fields {
		parts = append(parts, f.Key+"="+f.Value)
	}
	return strings.Join(parts, " ")
}

type state struct {
	mu      sync.Mutex
	ring    []Event
	limit   int
	next    uint64
	subs    map[int]chan Event
	nextSub int
}

type Recorder struct {
	inner  slog.Handler
	attrs  []slog.Attr
	groups []string
	state  *state
}

func NewRecorder(inner slog.Handler, limit int) *Recorder {
	if limit <= 0 {
		limit = 512
	}
	return &Recorder{
		inner: inner,
		state: &state{
			ring:  make([]Event, 0, limit),
			limit: limit,
			subs:  make(map[int]chan Event),
		},
	}
}

func (r *Recorder) Enabled(ctx context.Context, level slog.Level) bool {
	return r.inner.Enabled(ctx, level)
}

func (r *Recorder) Handle(ctx context.Context, rec slog.Record) error {
	fields := make([]Field, 0, len(r.attrs)+rec.NumAttrs())
	for _, a := range r.attrs {
		fields = appendAttr(fields, r.groups, a)
	}
	rec.Attrs(func(a slog.Attr) bool {
		fields = appendAttr(fields, r.groups, a)
		return true
	})

	r.state.publish(Event{
		Time:    rec.Time,
		Level:   rec.Level.String(),
		Message: rec.Message,
		Fields:  fields,
	})

	return r.inner.Handle(ctx, rec)
}

func (r *Recorder) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := *r
	next.inner = r.inner.WithAttrs(attrs)
	next.attrs = append(append([]slog.Attr(nil), r.attrs...), attrs...)
	return &next
}

func (r *Recorder) WithGroup(name string) slog.Handler {
	next := *r
	next.inner = r.inner.WithGroup(name)
	next.groups = append(append([]string(nil), r.groups...), name)
	return &next
}

func (r *Recorder) Snapshot() []Event {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	return append([]Event(nil), r.state.ring...)
}

func (r *Recorder) Latest() uint64 {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	return r.state.next
}

func (r *Recorder) Since(seq uint64) []Event {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()

	out := make([]Event, 0, len(r.state.ring))
	for _, e := range r.state.ring {
		if e.Seq > seq {
			out = append(out, e)
		}
	}
	return out
}

func (r *Recorder) Subscribe() (<-chan Event, func()) {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()

	id := r.state.nextSub
	r.state.nextSub++
	ch := make(chan Event, 256)
	r.state.subs[id] = ch

	return ch, func() {
		r.state.mu.Lock()
		defer r.state.mu.Unlock()
		if existing, ok := r.state.subs[id]; ok {
			delete(r.state.subs, id)
			close(existing)
		}
	}
}

func (s *state) publish(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.next++
	e.Seq = s.next

	if len(s.ring) == s.limit {
		copy(s.ring, s.ring[1:])
		s.ring[len(s.ring)-1] = e
	} else {
		s.ring = append(s.ring, e)
	}

	for _, ch := range s.subs {
		select {
		case ch <- e:
		default:
		}
	}
}

func appendAttr(dst []Field, groups []string, a slog.Attr) []Field {
	a.Value = a.Value.Resolve()

	if a.Value.Kind() == slog.KindGroup {
		prefix := append(append([]string(nil), groups...), a.Key)
		for _, nested := range a.Value.Group() {
			dst = appendAttr(dst, prefix, nested)
		}
		return dst
	}

	key := a.Key
	if len(groups) > 0 {
		key = strings.Join(groups, ".") + "." + key
	}
	if a.Key == "" {
		return dst
	}

	return append(dst, Field{Key: key, Value: fmt.Sprint(a.Value.Any())})
}
