package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"goa.design/ponos/streaming"
)

func main() {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: os.Getenv("REDIS_PASSWORD")})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	// Create stream
	stream, err := streaming.NewStream(ctx, "my-stream", rdb)
	if err != nil {
		panic(err)
	}
	defer stream.Destroy(ctx)

	// Add a new event
	if _, err := stream.Add(ctx, "event", []byte("payload")); err != nil {
		panic(err)
	}

	// Create reader that reads from the beginning and waits for events for
	// up to 100ms
	reader, err := stream.NewReader(ctx,
		streaming.WithReaderStartAtOldest(),
		streaming.WithReaderBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	// Consume event
	event := <-reader.C
	fmt.Printf("event: %s, payload: %s\n", event.EventName, event.Payload)
}
