package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/redis/go-redis/v9"
	"goa.design/pulse/rmap"
)

// Number of items to write to the map
const numitems = 9

func main() {
	ctx := context.Background()

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: os.Getenv("REDIS_PASSWORD")})

	// Make sure Redis is up and running and we can connect to it
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	// Join replicated map
	m, err := rmap.Join(ctx, "multinodes", rdb)
	if err != nil {
		panic(err)
	}

	if len(os.Args) > 1 && os.Args[1] == "--write" {
		// Reset the map
		if err := m.Reset(ctx); err != nil {
			panic(err)
		}

		// Send updates
		for i := 0; i < numitems; i++ {
			if _, err := m.Set(ctx, "foo-"+strconv.Itoa(i+1), "bar"); err != nil {
				panic(err)
			}
		}

		// Exit
		os.Exit(0)
	}

	// Start a goroutine to listen for updates
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := m.Subscribe()
		for range c {
			fmt.Printf("%q: %v keys\n", m.Name, m.Len())
			if m.Len() == numitems {
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
