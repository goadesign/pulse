package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"goa.design/ponos/streaming"
)

// Note: the example below does not handle errors for brevity.
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
	// Create sink
	sink, _ := stream.NewSink(ctx, "my-sink",
		streaming.WithSinkStartAtOldest(),
		streaming.WithSinkBlockDuration(100*time.Millisecond))
	defer sink.Close()
	// Create stream "my-other-stream"
	otherStream, _ := streaming.NewStream(ctx, "my-other-stream", rdb)
	defer otherStream.Destroy(ctx)
	// Add stream "my-other-stream" to sink "my-sink"
	sink.AddStream(ctx, otherStream)
	// Write event to stream "my-stream"
	stream.Add(ctx, "event 1", []byte("payload 1"))
	// Write event to stream "my-other-stream"
	otherStream.Add(ctx, "event 2", []byte("payload 2"))
	// Consume events from both streams
	event := <-sink.C
	fmt.Printf("stream: %s, event: %s, payload: %s\n", event.StreamName, event.EventName, event.Payload)
	sink.Ack(ctx, event)
	event = <-sink.C
	fmt.Printf("stream: %s, event: %s, payload: %s\n", event.StreamName, event.EventName, event.Payload)
	sink.Ack(ctx, event)
}
