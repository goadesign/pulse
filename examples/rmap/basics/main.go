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
	defer m.Close()

	// Reset the map
	if err := m.Reset(ctx); err != nil {
		panic(err)
	}

	// Write a string
	if _, err := m.Set(ctx, "stringval", "bar"); err != nil {
		panic(err)
	}

	// Set returns the previous value
	old, err := m.Set(ctx, "stringval", "baz")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%q: old value for stringval: %v\n", m.Name, old)

	// Delete a key
	prev, err := m.Delete(ctx, "stringval")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%q: deleted stringval: %v\n", m.Name, prev)

	// Append to a list
	if _, err := m.AppendValues(ctx, "listval", "first", "second"); err != nil {
		panic(err)
	}

	// Append returns the new values of the list
	vals, err := m.AppendValues(ctx, "listval", "third")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%q: new values for listval: %v\n", m.Name, vals)

	// Remove elements from a list and return the new content
	vals, err = m.RemoveValues(ctx, "listval", "first")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%q: new values for listval: %v\n", m.Name, vals)

	// Increment an integer, starts at 0
	if _, err := m.Inc(ctx, "intval", 1); err != nil {
		panic(err)
	}

	// Increment returns the new value
	val, err := m.Inc(ctx, "intval", 1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%q: new value for intval: %v\n", m.Name, val)

	// Print a single value
	st, ok := m.Get("stringval")
	fmt.Printf("%q: stringval: %v, ok: %v\n", m.Name, st, ok)

	// Print all keys
	keys := m.Keys()
	fmt.Printf("%q: keys: %v\n", m.Name, keys)

	// Print the number of items in the map
	ln := m.Len()
	fmt.Printf("%q: len: %v\n", m.Name, ln)

	// Print the final map
	content := m.Map()
	fmt.Printf("%q: final content: %v\n", m.Name, content)

	// Subscribe to changes and make sure to unsubscribe when done
	c := m.Subscribe()
	defer m.Unsubscribe(c)

	// Start a goroutine to listen for updates and stop when it gets one
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-c
	}()

	// Send updates
	if _, err := m.Set(ctx, "foo", "bar"); err != nil {
		panic(err)
	}

	// Wait for the updates to be received
	fmt.Println("waiting for updates...")
	wg.Wait()
	fmt.Printf("Got update, %q: %v\n", m.Name, m.Map())
}
