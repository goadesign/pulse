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

	// Make sure Redis is up and running and we can connect to it
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	// Create stream "my-stream"
	stream, err := streaming.NewStream(ctx, "my-stream", rdb)
	if err != nil {
		panic(err)
	}

	// Don't forget to destroy the stream when done
	defer stream.Destroy(ctx)

	// Create sink
	sink, err := stream.NewSink(ctx, "my-sink",
		streaming.WithSinkStartAtOldest(),
		streaming.WithSinkBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}

	// Don't forget to close the sink when done
	defer sink.Close()

	// Write event to stream "my-stream"
	id1, err := stream.Add(ctx, "event 1", []byte("payload 1"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("event 1 id: %s\n", id1)

	// Create stream "my-other-stream"
	otherStream, err := streaming.NewStream(ctx, "my-other-stream", rdb)
	if err != nil {
		panic(err)
	}
	defer otherStream.Destroy(ctx)

	// Add stream "my-other-stream" to sink "my-sink"
	err = sink.AddStream(ctx, otherStream)
	if err != nil {
		panic(err)
	}

	// Write event to stream "my-other-stream"
	id2, err := otherStream.Add(ctx, "event 2", []byte("payload 2"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("event 2 id: %s\n", id2)

	// Consume events from both streams
	event := <-sink.C
	fmt.Printf("stream: %s, event: %s, payload: %s\n", event.StreamName, event.EventName, event.Payload)
	if err := sink.Ack(ctx, event); err != nil {
		panic(err)
	}

	event = <-sink.C
	fmt.Printf("stream: %s, event: %s, payload: %s\n", event.StreamName, event.EventName, event.Payload)
	if err := sink.Ack(ctx, event); err != nil {
		panic(err)
	}

	// Streams can be removed from a sink
	err = sink.RemoveStream(ctx, otherStream)
	if err != nil {
		panic(err)
	}
}
