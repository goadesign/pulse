package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/redis/go-redis/v9"
	"goa.design/ponos/rmap"
)

func main() {
	ctx := context.Background()

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: os.Getenv("REDIS_PASSWORD")})

	// Make sure Redis is up and running and we can connect to it
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	// Join replicated map "my-map"
	m, err := rmap.Join(ctx, "my-map", rdb)
	if err != nil {
		panic(err)
	}

	// Create a channel to receive updates
	c := m.Subscribe()

	// Reset the map
	if err := m.Reset(ctx); err != nil {
		panic(err)
	}

	// Write a string
	if _, err := m.Set(ctx, "stringval", "bar"); err != nil {
		panic(err)
	}

	// Append to a list
	if _, err := m.AppendValues(ctx, "listval", "first", "second"); err != nil {
		panic(err)
	}

	// Remove elements from a list
	if _, err := m.RemoveValues(ctx, "listval", "first"); err != nil {
		panic(err)
	}

	// Increment an integer, starts at 0
	if _, err := m.Inc(ctx, "intval", 1); err != nil {
		panic(err)
	}

	// Start a goroutine to listen for updates and stop when the map has 3
	// keys
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range c {
			fmt.Printf("%q: %v keys\n", m.Name, m.Len())
			if m.Len() == 3 {
				break
			}
		}
	}()

	// Wait for the updates to be received
	fmt.Println("waiting for updates...")
	wg.Wait()

	// Print the final map
	fmt.Printf("%q: final content: %v\n", m.Name, m.Map())
}
