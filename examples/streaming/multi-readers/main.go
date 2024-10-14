package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"goa.design/pulse/streaming"
	"goa.design/pulse/streaming/options"
)

func main() {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: os.Getenv("REDIS_PASSWORD")})
	ctx := context.Background()

	// Make sure Redis is up and running and we can connect to it
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	// Create stream
	stream, err := streaming.NewStream("multireaders", rdb)
	if err != nil {
		panic(err)
	}

	// Don't forget to destroy the stream when done
	defer func() {
		if err := stream.Destroy(ctx); err != nil {
			panic(err)
		}
	}()

	// Write 2 events to the stream
	id1, err := stream.Add(ctx, "event 1", []byte("payload 1"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("event 1 id: %s\n", id1)

	id2, err := stream.Add(ctx, "event 2", []byte("payload 2"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("event 2 id: %s\n", id2)

	// Create reader1 for stream that reads from the beginning and
	// waits for events for up to 100ms
	reader1, err := stream.NewReader(ctx,
		options.WithReaderStartAtOldest(),
		options.WithReaderBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}

	// Don't forget to close the reader when done
	defer reader1.Close()

	// Read event
	ev := <-reader1.Subscribe()
	fmt.Printf("reader 1, event: %s, payload: %s\n", ev.EventName, ev.Payload)

	// Create other reader for stream and start reading after first
	// event
	reader2, err := stream.NewReader(ctx,
		options.WithReaderStartAfter(ev.ID),
		options.WithReaderBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}
	defer reader2.Close()

	// Read second event with other reader
	ev = <-reader2.Subscribe()
	fmt.Printf("reader 2, event: %s, payload: %s\n", ev.EventName, ev.Payload)
}
