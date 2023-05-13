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
	// Add a new event to topic "my-topic"
	stream.Add(ctx, "event 1", []byte("payload 2"), streaming.WithTopic("my-topic"))
	// Add a new event to topic "my-other-topic"
	stream.Add(ctx, "event 2", []byte("payload 2"), streaming.WithTopic("my-other-topic"))
	// Create sink for stream "my-stream" that reads from the beginning and
	// waits for events for up to 100ms
	sink, _ := stream.NewSink(ctx, "my-sink",
		streaming.WithSinkStartAtOldest(),
		streaming.WithSinkBlockDuration(100*time.Millisecond))
	defer sink.Close()
	// Read both events
	event := <-sink.C
	fmt.Printf("topic: %s, event: %s, payload: %s\n", event.Topic, event.EventName, event.Payload)
	sink.Ack(ctx, event)
	event = <-sink.C
	fmt.Printf("topic: %s, event: %s, payload: %s\n", event.Topic, event.EventName, event.Payload)
	sink.Ack(ctx, event)
}
