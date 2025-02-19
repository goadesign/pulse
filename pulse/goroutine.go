package pulse

import (
	"fmt"
	"runtime/debug"
)

// Go runs the given function in a separate goroutine and recovers from any panic,
// logging the panic message and stack trace.
//
// Usage:
//
//	Go(ctx, func() {
//	    // Your code here
//	})
func Go(logger Logger, f func()) {
	go func(logger Logger) {
		defer func() {
			if r := recover(); r != nil {
				panicErr := fmt.Errorf("Panic recovered: %v\n%s", r, debug.Stack())
				logger.Error(panicErr)
			}
		}()
		f()
	}(logger)
}
