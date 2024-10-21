package pulse

import (
	"context"
	"fmt"
	"runtime/debug"

	"goa.design/clue/log"
)

// Go runs the given function in a separate goroutine and recovers from any panic,
// logging the panic message and stack trace.
//
// Usage:
//
//	Go(ctx, func() {
//	    // Your code here
//	})
func Go(ctx context.Context, f func()) {
	go func() {
		defer func(ctx context.Context) {
			if r := recover(); r != nil {
				panicErr := fmt.Errorf("Panic recovered: %v\n%s", r, debug.Stack())
				log.Error(ctx, panicErr)
			}
		}(ctx)
		f()
	}()
}
