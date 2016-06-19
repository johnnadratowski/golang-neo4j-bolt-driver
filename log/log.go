package log

import (
	"fmt"
	l "log"
	"os"
	"strings"
)

type LogLevel int

const (
	NoneLevel  LogLevel = iota
	ErrorLevel LogLevel = iota
	InfoLevel  LogLevel = iota
	TraceLevel LogLevel = iota
)

var (
	Level    = NoneLevel
	TraceLog = l.New(os.Stderr, "[BOLT][TRACE]", l.LstdFlags)
	InfoLog  = l.New(os.Stderr, "[BOLT][INFO]", l.LstdFlags)
	ErrorLog = l.New(os.Stderr, "[BOLT][ERROR]", l.LstdFlags)
)

func SetLevel(level string) {
	switch strings.ToLower(level) {
	case "trace":
		Level = TraceLevel
	case "info":
		Level = InfoLevel
	case "error":
		Level = ErrorLevel
	default:
		Level = NoneLevel
	}
}

func Trace(args ...interface{}) {
	if Level >= TraceLevel {
		TraceLog.Println(args...)
	}
}

func Tracef(msg string, args ...interface{}) {
	if Level >= TraceLevel {
		TraceLog.Printf(msg, args...)
	}
}

func Info(args ...interface{}) {
	if Level >= InfoLevel {
		InfoLog.Println(args...)
	}
}

func Infof(msg string, args ...interface{}) {
	if Level >= InfoLevel {
		InfoLog.Printf(msg, args...)
	}
}

func Error(args ...interface{}) {
	if Level >= ErrorLevel {
		ErrorLog.Println(args...)
	}
}

func Errorf(msg string, args ...interface{}) {
	if Level >= ErrorLevel {
		ErrorLog.Printf(msg, args...)
	}
}

func Fatal(args ...interface{}) {
	if Level >= ErrorLevel {
		ErrorLog.Println(args...)
		os.Exit(1)
	}
}

func Fatalf(msg string, args ...interface{}) {
	if Level >= ErrorLevel {
		ErrorLog.Printf(msg, args...)
		os.Exit(1)
	}
}

func Panic(args ...interface{}) {
	if Level >= ErrorLevel {
		ErrorLog.Println(args...)
		panic(fmt.Sprint(args...))
	}
}

func Panicf(msg string, args ...interface{}) {
	if Level >= ErrorLevel {
		ErrorLog.Printf(msg, args...)
		panic(fmt.Sprintf(msg, args...))
	}
}
