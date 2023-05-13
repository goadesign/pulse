package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"goa.design/ponos/streaming"
)

// NOTE: the example below does not handle errors for brevity.
func main() {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: os.Getenv("REDIS_PASSWORD")})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}
	// Create stream
	stream, _ := streaming.NewStream(ctx, "my-stream", rdb)
	defer stream.Destroy(ctx)
	// Write 2 events to the stream
	stream.Add(ctx, "event 1", []byte("payload 1"))
	stream.Add(ctx, "event 2", []byte("payload 2"))
	// Create reader for stream "my-stream" that reads from the beginning and
	// waits for events for up to 100ms
	reader, _ := stream.NewReader(ctx,
		streaming.WithReaderStartAtOldest(),
		streaming.WithReaderBlockDuration(100*time.Millisecond))
	defer reader.Close()
	// Read both events
	event := <-reader.C
	fmt.Printf("reader 1, event: %s, payload: %s\n", event.EventName, event.Payload)
	event = <-reader.C
	fmt.Printf("reader 1, event: %s, payload: %s\n", event.EventName, event.Payload)
	// Create reader for stream "my-stream" and start reading after first event
	otherReader, _ := stream.NewReader(ctx,
		streaming.WithReaderStartAfter(event.ID),
		streaming.WithReaderBlockDuration(100*time.Millisecond))
	defer otherReader.Close()
	// Read second event
	event = <-otherReader.C
	fmt.Printf("reader 2, event: %s, payload: %s\n", event.EventName, event.Payload)
}
