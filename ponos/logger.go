package ponos

import (
	"context"

	stdlog "log"

	cluelog "goa.design/clue/log"
)

type (
	// Interface used by Ponos to write log entries.
	Logger interface {
		EnableDebug()
		Debug(format string, args ...any)
		Info(format string, args ...any)
		Error(err error)
	}

	// NilLogger is a no-op logger.
	NilLogger struct{}

	// stdLogger is a Go standard library logger adapter.
	stdLogger struct {
		debugEnabled bool
		logger       *stdlog.Logger
	}

	// clueLogger is a clue logger adapter.
	clueLogger struct {
		logContext context.Context
	}
)

// AdaptStdLogger adapts a Go standard library logger to a ponos logger.
func AdaptStdLogger(logger *stdlog.Logger) Logger {
	return &stdLogger{logger: logger}
}

// AdaptClueLogger adapts a clue logger to a ponos logger.
func AdaptClueLogger(logCtx context.Context) Logger {
	cluelog.MustContainLogger(logCtx)
	return &clueLogger{logCtx}
}

func (l *NilLogger) EnableDebug()             {}
func (l *NilLogger) Debug(_ string, _ ...any) {}
func (l *NilLogger) Info(_ string, _ ...any)  {}
func (l *NilLogger) Error(_ error)            {}

func (l *stdLogger) EnableDebug() {
	l.debugEnabled = true
}

func (l *stdLogger) Debug(format string, args ...any) {
	if l.debugEnabled {
		l.logger.Printf("[DEBUG] "+format, args...)
	}
}

func (l *stdLogger) Info(format string, args ...any) {
	l.logger.Printf("[INFO] "+format, args...)
}

func (l *stdLogger) Error(err error) {
	l.logger.Print("[ERROR] " + err.Error())
}

func (l *clueLogger) EnableDebug() {
	l.logContext = cluelog.Context(l.logContext, cluelog.WithDebug())
}

func (l *clueLogger) Debug(format string, args ...any) {
	cluelog.Debugf(l.logContext, format, args...)
}

func (l *clueLogger) Info(format string, args ...any) {
	cluelog.Infof(l.logContext, format, args...)
}

func (l *clueLogger) Error(err error) {
	cluelog.Error(l.logContext, err)
}
