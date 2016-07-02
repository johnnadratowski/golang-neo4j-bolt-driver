package log

import (
	l "log"
	"os"
	"strings"
)

// Level is the logging level
type Level int

const (
	// NoneLevel is no logging
	NoneLevel Level = iota
	// ErrorLevel is error logging
	ErrorLevel Level = iota
	// InfoLevel is info logging
	InfoLevel Level = iota
	// TraceLevel is trace logging
	TraceLevel Level = iota
)

var (
	level = NoneLevel
	// ErrorLog is the logger for error logging. This can be manually overridden.
	ErrorLog = l.New(os.Stderr, "[BOLT][ERROR]", l.LstdFlags)
	// InfoLog is the logger for info logging. This can be manually overridden.
	InfoLog = l.New(os.Stderr, "[BOLT][INFO]", l.LstdFlags)
	// TraceLog is the logger for trace logging. This can be manually overridden.
	TraceLog = l.New(os.Stderr, "[BOLT][TRACE]", l.LstdFlags)
)

// SetLevel sets the logging level of this package. levelStr should be one of "trace", "info", or "error
func SetLevel(levelStr string) {
	switch strings.ToLower(levelStr) {
	case "trace":
		level = TraceLevel
	case "info":
		level = InfoLevel
	case "error":
		level = ErrorLevel
	default:
		level = NoneLevel
	}
}

// GetLevel gets the logging level
func GetLevel() Level {
	return level
}

// Trace writes a trace log in the format of Println
func Trace(args ...interface{}) {
	if level >= TraceLevel {
		TraceLog.Println(args...)
	}
}

// Tracef writes a trace log in the format of Printf
func Tracef(msg string, args ...interface{}) {
	if level >= TraceLevel {
		TraceLog.Printf(msg, args...)
	}
}

// Info writes an info log in the format of Println
func Info(args ...interface{}) {
	if level >= InfoLevel {
		InfoLog.Println(args...)
	}
}

// Infof writes an info log in the format of Printf
func Infof(msg string, args ...interface{}) {
	if level >= InfoLevel {
		InfoLog.Printf(msg, args...)
	}
}

// Error writes an error log in the format of Println
func Error(args ...interface{}) {
	if level >= ErrorLevel {
		ErrorLog.Println(args...)
	}
}

// Errorf writes an error log in the format of Printf
func Errorf(msg string, args ...interface{}) {
	if level >= ErrorLevel {
		ErrorLog.Printf(msg, args...)
	}
}

// Fatal writes an error log in the format of Fatalln
func Fatal(args ...interface{}) {
	l.Fatalln(args...)
}

// Fatalf writes an error log in the format of Fatalf
func Fatalf(msg string, args ...interface{}) {
	l.Fatalf(msg, args...)
}

// Panic writes an error log in the format of Panicln
func Panic(args ...interface{}) {
	l.Panicln(args...)
}

// Panicf writes an error log in the format of Panicf
func Panicf(msg string, args ...interface{}) {
	l.Panicf(msg, args...)
}
