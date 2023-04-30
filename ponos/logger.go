package ponos

import (
	"context"

	stdlog "log"

	cluelog "goa.design/clue/log"
)

type (
	// Interface used by Ponos to write log entries.
	Logger interface {
		// EnableDebug turns on debug logging.
		EnableDebug()
		// WithPrefix returns a logger that prefixes all log entries with the
		// given key-value pairs.
		WithPrefix(kvs ...any) Logger
		// Debug logs a debug message.
		Debug(msg string, kvs ...any)
		// Info logs an info message.
		Info(msg string, kvs ...any)
		// Error logs an error message.
		Error(err error, kvs ...any)
	}

	// noopLogger is a no-op logger.
	noopLogger struct{}

	// stdLogger is a Go standard library logger adapter.
	stdLogger struct {
		debugEnabled bool
		prefix       string
		logger       *stdlog.Logger
	}

	// clueLogger is a clue logger adapter.
	clueLogger struct {
		logContext context.Context
	}
)

var (
	_ Logger = (*noopLogger)(nil)
	_ Logger = (*stdLogger)(nil)
	_ Logger = (*clueLogger)(nil)
)

// NoopLogger returns a no-op logger.
func NoopLogger() Logger {
	return &noopLogger{}
}

// StdLogger adapts a Go standard library logger to a ponos logger.
func StdLogger(logger *stdlog.Logger) Logger {
	return &stdLogger{logger: logger}
}

// ClueLogger adapts a clue logger to a ponos logger.
func ClueLogger(logCtx context.Context) Logger {
	cluelog.MustContainLogger(logCtx)
	return &clueLogger{logCtx}
}

func (l *noopLogger) EnableDebug()               {}
func (l *noopLogger) WithPrefix(_ ...any) Logger { return l }
func (l *noopLogger) Debug(_ string, _ ...any)   {}
func (l *noopLogger) Info(_ string, _ ...any)    {}
func (l *noopLogger) Error(_ error, _ ...any)    {}

func (l *stdLogger) EnableDebug() {
	l.debugEnabled = true
}

func (l *stdLogger) WithPrefix(kvs ...any) Logger {
	return &stdLogger{
		debugEnabled: l.debugEnabled,
		prefix:       l.prefix + format(kvs) + " ",
		logger:       l.logger,
	}
}

func (l *stdLogger) Debug(msg string, kvs ...any) {
	if l.debugEnabled {
		l.logger.Printf("[DEBUG] "+l.prefix+msg+format(kvs), args(kvs)...)
	}
}

func (l *stdLogger) Info(msg string, kvs ...any) {
	l.logger.Printf("[INFO] "+l.prefix+msg+format(kvs), args(kvs)...)
}

func (l *stdLogger) Error(err error, kvs ...any) {
	l.logger.Printf("[ERROR] "+err.Error()+" "+l.prefix+format(kvs), args(kvs)...)
}

func format(kvs []any) string {
	var format string
	if len(kvs) > 0 {
		format = " "
	}
	for i := 0; i < len(kvs); i += 2 {
		format += "%v=%v "
	}
	if len(kvs) > 0 {
		format = format[:len(format)-1]
	}
	return format
}

func args(kvs []any) (args []any) {
	for i := 0; i < len(kvs); i += 2 {
		args = append(args, kvs[i], kvs[i+1])
	}
	return args
}

func (l *clueLogger) EnableDebug() {
	l.logContext = cluelog.Context(l.logContext, cluelog.WithDebug())
}

func (l *clueLogger) WithPrefix(kvs ...any) Logger {
	return &clueLogger{
		logContext: cluelog.With(l.logContext, toFields(kvs...)...),
	}
}

func (l *clueLogger) Debug(msg string, kvs ...any) {
	kvs = append([]any{"msg", msg}, kvs...)
	cluelog.Debug(l.logContext, toFields(kvs...)...)
}

func (l *clueLogger) Info(msg string, kvs ...any) {
	kvs = append([]any{"msg", msg}, kvs...)
	cluelog.Info(l.logContext, toFields(kvs...)...)
}

func (l *clueLogger) Error(err error, kvs ...any) {
	cluelog.Error(l.logContext, err, toFields(kvs...)...)
}

func toFields(kvs ...any) []cluelog.Fielder {
	fields := make([]cluelog.Fielder, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		fields[i/2] = cluelog.KV{K: kvs[i].(string), V: kvs[i+1]}
	}
	return fields
}
