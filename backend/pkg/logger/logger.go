// backend/pkg/logger/logger.go
package logger

import (
	"log"
	"os"
)

// Logger is a wrapper around the standard log.Logger
type Logger struct {
	*log.Logger
}

// New creates a new logger instance writing to stdout with a standard prefix.
func New(prefix string) *Logger {
	return &Logger{
		Logger: log.New(os.Stdout, prefix, log.LstdFlags|log.Lshortfile),
	}
}

// Info logs an informational message.
func (l *Logger) Info(v ...interface{}) {
	l.SetPrefix("INFO: ")
	l.Println(v...)
}

// Error logs an error message.
func (l *Logger) Error(v ...interface{}) {
	l.SetPrefix("ERROR: ")
	l.Println(v...)
}

// Warn logs a warning message.
func (l *Logger) Warn(v ...interface{}) {
	l.SetPrefix("WARN: ")
	l.Println(v...)
}
