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
	// Create sink for stream "my-stream" and read from the beginning
	sink, _ := stream.NewSink(ctx, "my-sink",
		streaming.WithSinkStartAtOldest(),
		streaming.WithSinkBlockDuration(100*time.Millisecond))
	defer sink.Close()
	// Read both events
	event := <-sink.C
	fmt.Printf("sink 1, event: %s, payload: %s\n", event.EventName, event.Payload)
	sink.Ack(ctx, event)
	event = <-sink.C
	fmt.Printf("sink 1, event: %s, payload: %s\n", event.EventName, event.Payload)
	sink.Ack(ctx, event)
	// Create sink for stream "my-stream" and start reading after first event
	otherSink, _ := stream.NewSink(ctx, "other", streaming.WithSinkStartAfter(event.ID))
	defer otherSink.Close()
	// Read second event
	event = <-otherSink.C
	fmt.Printf("sink 2, event: %s, payload: %s\n", event.EventName, event.Payload)
	otherSink.Ack(ctx, event)
}
