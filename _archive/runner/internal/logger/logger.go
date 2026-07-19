package logger

import (
	"log"
	"os"
)

// Logger provides structured logging capabilities
type Logger struct {
	infoLog  *log.Logger
	errorLog *log.Logger
	warnLog  *log.Logger
	debugLog *log.Logger
}

// New creates a new logger instance
func New() *Logger {
	return &Logger{
		infoLog:  log.New(os.Stdout, "[INFO] ", log.LstdFlags|log.LUTC),
		errorLog: log.New(os.Stderr, "[ERROR] ", log.LstdFlags|log.LUTC),
		warnLog:  log.New(os.Stdout, "[WARN] ", log.LstdFlags|log.LUTC),
		debugLog: log.New(os.Stdout, "[DEBUG] ", log.LstdFlags|log.LUTC),
	}
}

// Info logs an info message
func (l *Logger) Info(format string, v ...interface{}) {
	l.infoLog.Printf(format, v...)
}

// Error logs an error message
func (l *Logger) Error(format string, v ...interface{}) {
	l.errorLog.Printf(format, v...)
}

// Warn logs a warning message
func (l *Logger) Warn(format string, v ...interface{}) {
	l.warnLog.Printf(format, v...)
}

// Debug logs a debug message
func (l *Logger) Debug(format string, v ...interface{}) {
	l.debugLog.Printf(format, v...)
}

// Fatal logs a fatal error and exits
func (l *Logger) Fatal(format string, v ...interface{}) {
	l.errorLog.Fatalf(format, v...)
}

