package main

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
	"goa.design/ponos/streaming"
)

// NOTE: the example below does not handle errors for brevity.
func main() {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: os.Getenv("REDIS_PASSWORD")})
	// Create stream
	ctx := context.Background()
	stream, _ := streaming.NewStream(ctx, "my-stream", rdb)
	// Write 2 events to the stream
	stream.Add(ctx, "event1", []byte("payload1"))
	stream.Add(ctx, "event2", []byte("payload2"))
	// Create reader for stream "my-stream" and read from the beginning
	reader, _ := stream.NewReader(ctx, streaming.WithReaderStartAtOldest())
	defer reader.Close()
	// Read both events
	event := <-reader.C
	fmt.Printf("reader 1, event: %s, payload: %s\n", event.EventName, event.Payload)
	event = <-reader.C
	fmt.Printf("reader 1, event: %s, payload: %s\n", event.EventName, event.Payload)
	// Create reader for stream "my-stream" and start reading after first event
	otherReader, _ := stream.NewReader(ctx, streaming.WithReaderStartAfter(event.ID))
	defer otherReader.Close()
	// Read second event
	event = <-otherReader.C
	fmt.Printf("reader 2, event: %s, payload: %s\n", event.EventName, event.Payload)
}
