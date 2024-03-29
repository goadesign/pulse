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
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	// Create stream
	stream, err := streaming.NewStream("sr-stream", rdb)
	if err != nil {
		panic(err)
	}

	// Don't forget to destroy the stream when done
	defer stream.Destroy(ctx)

	// Add a new event
	id, err := stream.Add(ctx, "event", []byte("payload"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("event id: %s\n", id)

	// Create reader that reads from the beginning and waits for
	// events for up to 100ms
	reader, err := stream.NewReader(ctx,
		options.WithReaderStartAtOldest(),
		options.WithReaderBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}

	// Don't forget to close the reader when done
	defer reader.Close()

	// Consume event
	ev := <-reader.Subscribe()
	fmt.Printf("event: %s, payload: %s\n", ev.EventName, ev.Payload)
}
