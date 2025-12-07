package logging

import (
	"log/slog"
	"os"
)

type StructuredLogger struct {
	base *slog.Logger
}

func NewStructuredLogger() *StructuredLogger {
	return &StructuredLogger{
		base: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}
}

func  (sl *StructuredLogger) WithTrace(traceID, component, method, endpoint string) *slog.Logger {
	return sl.base.With("trace_id", traceID, "component", component, "method", method, "endpoint", endpoint)
}