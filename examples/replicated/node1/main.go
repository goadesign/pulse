package main

import (
	"context"
	"os"
	"strconv"
	"sync"

	"github.com/redis/go-redis/v9"
	"goa.design/clue/log"
	"goa.design/ponos/ponos"
	"goa.design/ponos/replicated"
)

func main() {
	// Create a Redis client
	pwd := os.Getenv("REDIS_PASSWORD")
	if pwd == "" {
		panic("REDIS_PASSWORD not set")
	}
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: pwd,
	})

	ctx, cancel := context.WithCancel(context.Background())
	// Cleanup when done
	defer cancel()

	// Join or create a replicated map
	logCtx := log.Context(ctx)
	log.FlushAndDisableBuffering(logCtx)
	logger := ponos.AdaptClueLogger(logCtx)

	m, err := replicated.Join(ctx, "my-map", client, replicated.WithLogger(logger))
	if err != nil {
		panic(err)
	}

	// Add a new key
	if err := m.Set(ctx, "foo", "bar"); err != nil {
		panic(err)
	}

	// Keys set by the current process are available immediately
	_, ok := m.Get("foo")
	if !ok {
		panic("key not found")
	}

	// Reset the map
	if err = m.Reset(ctx); err != nil {
		panic(err)
	}

	// Start a goroutine to listen for updates
	numitems := 10
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.C:
				if len(m.Map()) == numitems {
					return
				}
			}
		}
	}()

	// Send a few updates
	for i := 0; i < numitems; i++ {
		m.Set(ctx, "foo-"+strconv.Itoa(i+1), "bar")
	}

	// Wait for the updates to be received
	wg.Wait()
}
