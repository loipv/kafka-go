package kafka

import (
	"log"
	"os"
)

// Logger interface for customizable logging
type Logger interface {
	Debug(format string, args ...any)
	Info(format string, args ...any)
	Warn(format string, args ...any)
	Error(format string, args ...any)
}

// DefaultLogger implements Logger using standard log package
type DefaultLogger struct {
	level  LogLevel
	logger *log.Logger
}

// NewDefaultLogger creates a new default logger
func NewDefaultLogger(level LogLevel) *DefaultLogger {
	return &DefaultLogger{
		level:  level,
		logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Debug logs a debug message
func (l *DefaultLogger) Debug(format string, args ...any) {
	if l.level >= LogLevelDebug {
		l.logger.Printf("[DEBUG] "+format, args...)
	}
}

// Info logs an info message
func (l *DefaultLogger) Info(format string, args ...any) {
	if l.level >= LogLevelInfo {
		l.logger.Printf("[INFO] "+format, args...)
	}
}

// Warn logs a warning message
func (l *DefaultLogger) Warn(format string, args ...any) {
	if l.level >= LogLevelWarn {
		l.logger.Printf("[WARN] "+format, args...)
	}
}

// Error logs an error message
func (l *DefaultLogger) Error(format string, args ...any) {
	if l.level >= LogLevelError {
		l.logger.Printf("[ERROR] "+format, args...)
	}
}

// NoopLogger is a logger that does nothing
type NoopLogger struct{}

// NewNoopLogger creates a no-op logger
func NewNoopLogger() *NoopLogger {
	return &NoopLogger{}
}

// Debug does nothing
func (l *NoopLogger) Debug(format string, args ...any) {}

// Info does nothing
func (l *NoopLogger) Info(format string, args ...any) {}

// Warn does nothing
func (l *NoopLogger) Warn(format string, args ...any) {}

// Error does nothing
func (l *NoopLogger) Error(format string, args ...any) {}
