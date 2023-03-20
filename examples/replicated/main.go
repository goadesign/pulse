package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/redis/go-redis/v9"
	"goa.design/ponos/replicated"
)

func main() {
	// Create a Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: os.Getenv("REDIS_PASSWORD"),
	})

	ctx, cancel := context.WithCancel(context.Background())
	// Cleanup when done
	defer cancel()

	// Join or create a replicated map
	m, err := replicated.Join(ctx, "my-map", client)
	if err != nil {
		panic(err)
	}

	// Print the current contents of the map
	fmt.Println(m.Map())

	// Add a new key
	m.Set(ctx, "foo", "bar")

	// Keys set by the current process are available immediately
	val, ok := m.Get("foo")
	fmt.Println(val, ok)

	// Start a goroutine to listen for updates
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.C:
				fmt.Println("map updated:", m.Map())
				if len(m.Map()) == 100 {
					wg.Done()
				}
			}
		}
	}()

	// Send a few updates
	for i := 0; i < 100; i++ {
		m.Set(ctx, "foo-"+strconv.Itoa(i+1), "bar")
	}

	// Wait for the updates to be received
	wg.Wait()
}
