package pulse

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"goa.design/clue/log"
)

func TestGo(t *testing.T) {
	t.Run("executes function without panic", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)
		executed := false

		Go(NoopLogger(), func() {
			defer wg.Done()
			executed = true
		})

		wg.Wait()
		assert.True(t, executed, "Function should have been executed")
	})

	t.Run("recovers from panic and logs error", func(t *testing.T) {
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(1)

		var buf strings.Builder
		ctx = log.Context(ctx, log.WithOutput(&buf))
		logger := ClueLogger(ctx)

		Go(logger, func() {
			defer wg.Done()
			panic("test panic")
		})

		wg.Wait()

		// Use eventually to allow for asynchronous logging
		assert.Eventually(t, func() bool {
			logOutput := buf.String()
			return strings.Contains(logOutput, "Panic recovered: test panic") &&
				strings.Contains(logOutput, "goroutine.go")
		}, 100*time.Millisecond, 10*time.Millisecond, "Log should contain panic message and stack trace")
	})

	t.Run("handles non-string panic values", func(t *testing.T) {
		ctx := context.Background()
		var wg sync.WaitGroup
		wg.Add(1)

		var buf strings.Builder
		ctx = log.Context(ctx, log.WithOutput(&buf))
		logger := ClueLogger(ctx)

		Go(logger, func() {
			defer wg.Done()
			panic(errors.New("custom error"))
		})

		wg.Wait()
		assert.Eventually(t, func() bool {
			logOutput := buf.String()
			return strings.Contains(logOutput, "Panic recovered: custom error") &&
				strings.Contains(logOutput, "goroutine.go")
		}, 100*time.Millisecond, 10*time.Millisecond, "Log should contain panic message and stack trace")
	})
}
