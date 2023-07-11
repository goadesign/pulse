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

// Note: the example below does not handle errors for brevity.
func main() {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: os.Getenv("REDIS_PASSWORD")})
	ctx := context.Background()

	// Make sure Redis is up and running and we can connect to it
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	// Create stream
	stream1, err := streaming.NewStream("multistreams-stream1", rdb)
	if err != nil {
		panic(err)
	}

	// Don't forget to destroy the stream when done
	defer stream1.Destroy(ctx)

	// Create sink
	sink, err := stream1.NewSink(ctx, "multistreams-sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}

	// Don't forget to close the sink when done
	defer sink.Close()

	// Subscribe to events
	c := sink.Subscribe()

	// Write event to stream
	id1, err := stream1.Add(ctx, "event 1", []byte("payload 1"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("event 1 id: %s\n", id1)

	// Create second stream
	stream2, err := streaming.NewStream("multistreams-stream2", rdb)
	if err != nil {
		panic(err)
	}
	defer stream2.Destroy(ctx)

	// Add stream to sink
	err = sink.AddStream(ctx, stream2)
	if err != nil {
		panic(err)
	}

	// Write event to stream
	id2, err := stream2.Add(ctx, "event 2", []byte("payload 2"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("event 2 id: %s\n", id2)

	// Consume events from both streams
	event := <-c
	fmt.Printf("stream: %s, event: %s, payload: %s\n", event.StreamName, event.EventName, event.Payload)
	if err := sink.Ack(ctx, event); err != nil {
		panic(err)
	}

	event = <-c
	fmt.Printf("stream: %s, event: %s, payload: %s\n", event.StreamName, event.EventName, event.Payload)
	if err := sink.Ack(ctx, event); err != nil {
		panic(err)
	}

	// Streams can be removed from a sink
	err = sink.RemoveStream(ctx, stream2)
	if err != nil {
		panic(err)
	}
}
