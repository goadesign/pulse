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

	// Create sink for stream "my-stream" that reads from the beginning and
	// waits for events for up to 100ms
	sink, err := stream.NewSink(ctx, "my-sink",
		streaming.WithSinkStartAtOldest(),
		streaming.WithSinkBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}

	// Don't forget to close the sink when done
	defer sink.Close()

	// Read and acknowlege event
	event := <-sink.Subscribe()
	fmt.Printf("sink 1, event: %s, payload: %s\n", event.EventName, event.Payload)
	if err := sink.Ack(ctx, event); err != nil {
		panic(err)
	}

	// Create sink for stream "my-stream" and start reading after first event
	otherSink, err := stream.NewSink(ctx, "other", streaming.WithSinkStartAfter(event.ID))
	if err != nil {
		panic(err)
	}
	defer otherSink.Close()

	// Read second event
	event = <-otherSink.Subscribe()
	fmt.Printf("sink 2, event: %s, payload: %s\n", event.EventName, event.Payload)
	if otherSink.Ack(ctx, event); err != nil {
		panic(err)
	}
}
