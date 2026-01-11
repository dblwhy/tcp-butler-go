package tcpbutler

import (
	"log/slog"
	"os"
)

//
// Logger abstraction
//

// Logger is a minimal structured logging interface for your library.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
	With(kv ...any) Logger
}

// SlogLogger is a Logger implementation backed by log/slog.
type SlogLogger struct {
	l *slog.Logger
}

func NewSlogLogger() *SlogLogger {
	return &SlogLogger{slog.Default()}
}

// NewSlogTextLogger creates a sane default text logger to stdout.
func NewSlogTextLogger() *SlogLogger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	return NewSlogLogger(handler)
}

func (s *SlogLogger) Debug(msg string, kv ...any) { s.l.Debug(msg, kv...) }
func (s *SlogLogger) Info(msg string, kv ...any)  { s.l.Info(msg, kv...) }
func (s *SlogLogger) Warn(msg string, kv ...any)  { s.l.Warn(msg, kv...) }
func (s *SlogLogger) Error(msg string, kv ...any) { s.l.Error(msg, kv...) }

func (s *SlogLogger) With(kv ...any) Logger {
	return &SlogLogger{l: s.l.With(kv...)}
}

// NopLogger drops all logs (useful for tests or as a default).
type NopLogger struct{}

func (NopLogger) Debug(string, ...any) {}
func (NopLogger) Info(string, ...any)  {}
func (NopLogger) Warn(string, ...any)  {}
func (NopLogger) Error(string, ...any) {}
func (NopLogger) With(...any) Logger   { return NopLogger{} }
