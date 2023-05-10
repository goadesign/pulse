package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/redis/go-redis/v9"
	"goa.design/clue/log"
	"goa.design/ponos/ponos"
	"goa.design/ponos/rmap"
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
	logger := ponos.ClueLogger(logCtx)

	m, err := rmap.Join(ctx, "my-map", client, rmap.WithLogger(logger))
	if err != nil {
		panic(err)
	}

	// Start a goroutine to listen for updates from node1
	numitems := 10
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := m.Subscribe()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c:
				if len(m.Map()) == numitems {
					return
				}
			}
		}
	}()

	// Wait for the updates from node1 to be received
	fmt.Println("Waiting for updates from node1...")
	wg.Wait()
}
