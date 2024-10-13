package testing

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"goa.design/clue/log"
)

// reset is the ANSI escape code for resetting the terminal color.
const reset = "\033[0m"

// epoch is the time used for formatting log timestamps.
var epoch = time.Now()

// NewTestContext returns a new context with a logger that outputs to the terminal.
func NewTestContext(t *testing.T) context.Context {
	t.Helper()
	if log.IsTerminal() {
		return log.Context(context.Background(), log.WithDebug(), log.WithFormat(FormatTerminal))
	}
	return log.Context(context.Background(), log.WithDebug())
}

// NewBufferedLogContext returns a new context and buffer for capturing log output.
func NewBufferedLogContext(t *testing.T) (context.Context, *Buffer) {
	t.Helper()
	var buf Buffer
	return log.Context(context.Background(), log.WithOutput(&buf), log.WithFormat(log.FormatText), log.WithDebug()), &buf
}

// FormatTerminal formats a log entry for terminal output.
// It differs from Clue's default terminal format in that it
// formats timestamps in milliseconds instead of seconds.
func FormatTerminal(e *log.Entry) []byte {
	var b bytes.Buffer
	b.WriteString(e.Severity.Color())
	b.WriteString(e.Severity.Code())
	b.WriteString(reset)
	b.WriteString(fmt.Sprintf("[%04d]", int(e.Time.Sub(epoch)/time.Millisecond)))
	if len(e.KeyVals) > 0 {
		b.WriteByte(' ')
		for i, kv := range e.KeyVals {
			b.WriteString(e.Severity.Color())
			b.WriteString(kv.K)
			b.WriteString(reset)
			b.WriteString(fmt.Sprintf("=%v", kv.V))
			if i < len(e.KeyVals)-1 {
				b.WriteByte(' ')
			}
		}
	}
	b.WriteByte('\n')
	return b.Bytes()
}
