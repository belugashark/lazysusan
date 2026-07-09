// Package logging provides a tiny JSON-line logger. Output goes to stdout so
// Railway and other log captures pick it up.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a slog.Logger writing JSON lines to stdout at the requested
// level. Unknown levels default to info.
func New(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{
		Level: lvl,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.MessageKey {
				return slog.Attr{Key: "msg", Value: a.Value}
			}
			if a.Key == slog.TimeKey {
				return slog.Attr{Key: "time", Value: a.Value}
			}
			if a.Key == slog.LevelKey {
				return slog.Attr{Key: "level", Value: slog.StringValue(strings.ToLower(a.Value.String()))}
			}
			return a
		},
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
