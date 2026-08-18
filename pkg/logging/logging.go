// Package logging
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/lmittmann/tint"
	"golang.org/x/term"
)

type Format string

const (
	FormatAuto    Format = "auto"
	FormatJSON    Format = "json"
	FormatText    Format = "text"
	FormatConsole Format = "console"
)

func Formats() []string {
	return []string{string(FormatAuto), string(FormatJSON), string(FormatText), string(FormatConsole)}
}

func Levels() []string {
	return []string{"debug", "info", "warn", "error"}
}

func ParseFormat(s string) (Format, error) {
	switch Format(strings.ToLower(strings.TrimSpace(s))) {
	case FormatAuto:
		return FormatAuto, nil
	case FormatJSON:
		return FormatJSON, nil
	case FormatText:
		return FormatText, nil
	case FormatConsole:
		return FormatConsole, nil
	default:
		return "", fmt.Errorf("unknown log format %q, want one of %s", s, strings.Join(Formats(), ", "))
	}
}

func ParseLevel(s string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.TrimSpace(s))); err != nil {
		return 0, fmt.Errorf("unknown log level %q, want one of %s", s, strings.Join(Levels(), ", "))
	}
	return level, nil
}

type Options struct {
	Format    Format
	Level     slog.Level
	Writer    io.Writer
	AddSource bool
	TimeFmt   string
}

func New(opts Options) *slog.Logger {
	return slog.New(NewHandler(opts))
}

func NewHandler(opts Options) slog.Handler {
	if opts.Writer == nil {
		opts.Writer = os.Stderr
	}
	if opts.TimeFmt == "" {
		opts.TimeFmt = time.TimeOnly
	}

	format := opts.Format
	if format == "" || format == FormatAuto {
		if isTerminal(opts.Writer) {
			format = FormatConsole
		} else {
			format = FormatJSON
		}
	}

	handlerOpts := &slog.HandlerOptions{Level: opts.Level, AddSource: opts.AddSource}

	var handler slog.Handler
	switch format {
	case FormatJSON:
		handler = slog.NewJSONHandler(opts.Writer, handlerOpts)
	case FormatText:
		handler = slog.NewTextHandler(opts.Writer, handlerOpts)
	default:
		handler = tint.NewTextHandler(opts.Writer, &tint.Options{
			Level:      opts.Level,
			AddSource:  opts.AddSource,
			TimeFormat: opts.TimeFmt,
			NoColor:    !isTerminal(opts.Writer),
		})
	}

	return handler
}

func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}
