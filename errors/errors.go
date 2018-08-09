package errors

import (
	"fmt"
	"runtime/debug"
	"strings"
)

// Error is the base error type adds stack trace and wrapping errors
type Error struct {
	msg     string
	wrapped error
	stack   []byte
	level   int
}

// New makes a new error
func New(msg string, args ...interface{}) *Error {
	return &Error{
		msg:   fmt.Sprintf(msg, args...),
		stack: debug.Stack(),
		level: 0,
	}
}

// Wrap wraps an error with a new error
func Wrap(err error, msg string, args ...interface{}) *Error {
	if e, ok := err.(*Error); ok {
		return &Error{
			msg:     fmt.Sprintf(msg, args...),
			wrapped: e,
		}
	}

	return &Error{
		msg:     fmt.Sprintf(msg, args...),
		wrapped: err,
		stack:   debug.Stack(),
	}
}

// Error gets the error output
func (e *Error) Error() string {
	return e.error(0)
}

// Inner returns the inner error wrapped by this error
func (e *Error) Inner() error {
	return e.wrapped
}

// InnerMost returns the innermost error wrapped by this error
func (e *Error) InnerMost() error {
	if e.wrapped == nil {
		return e
	}

	if inner, ok := e.wrapped.(*Error); ok {
		return inner.InnerMost()
	}

	return e.wrapped
}

func (e *Error) error(level int) string {
	msg := fmt.Sprintf("%s%s", strings.Repeat("\t", level), e.msg)
	if e.wrapped != nil {
		if wrappedErr, ok := e.wrapped.(*Error); ok {
			msg += fmt.Sprintf("\n%s", wrappedErr.error(level+1))
		} else {
			msg += fmt.Sprintf("\nInternal Error(%T):%s", e.wrapped, e.wrapped.Error())
		}
	}

	if len(e.stack) > 0 {
		msg += fmt.Sprintf("\n\n Stack Trace:\n\n%s", e.stack)
	}

	return msg
}
