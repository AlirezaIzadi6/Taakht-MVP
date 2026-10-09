package observe

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/taakht/taakht/libs/goplatform/identity"
)

// Environment variables read by SetupLogging.
const (
	EnvLogFormat = "LOG_FORMAT" // text (default) or json
	EnvLogLevel  = "LOG_LEVEL"  // debug, info (default), warn or error
)

// SetupLogging installs the process-wide slog logger: LOG_FORMAT=json writes one JSON object per line with
// the fields ts (UTC), level, service, msg plus request_id and user_id when the call context has them;
// anything else (the default) writes human-readable text. Log with the *Context variants to get the ids.
// Secrets, tokens and request payloads must never be passed to the logger.
func SetupLogging(service string) *slog.Logger {
	l := newLogger(service, os.Getenv(EnvLogFormat), os.Getenv(EnvLogLevel), os.Stderr)
	slog.SetDefault(l)
	return l
}

// NewLoggerForTest builds the logger SetupLogging installs, writing to w, without touching the process default.
func NewLoggerForTest(service, format, level string, w io.Writer) *slog.Logger {
	return newLogger(service, format, level, w)
}

func newLogger(service, format, level string, w io.Writer) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if strings.EqualFold(format, "json") {
		opts.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 {
				return a
			}
			switch a.Key {
			case slog.TimeKey:
				return slog.String("ts", a.Value.Time().UTC().Format(time.RFC3339Nano))
			case slog.LevelKey:
				return slog.String("level", strings.ToLower(a.Value.String()))
			}
			return a
		}
		h = slog.NewJSONHandler(w, opts).WithAttrs([]slog.Attr{slog.String("service", service)})
	} else {
		h = slog.NewTextHandler(w, opts).WithAttrs([]slog.Attr{slog.String("service", service)})
	}
	return slog.New(contextHandler{h})
}

// contextHandler adds request_id and user_id from the call context to every record.
type contextHandler struct{ slog.Handler }

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestID(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	if uid := identity.UserID(ctx); uid != "" && identity.Valid(uid) {
		r.AddAttrs(slog.String("user_id", uid))
	}
	return h.Handler.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(a)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}
