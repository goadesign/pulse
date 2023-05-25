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
	stream, err := streaming.NewStream(ctx, "singlesink-stream", rdb)
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

	// Create sink that reads from the beginning and waits for events for
	// up to 100ms
	sink, err := stream.NewSink(ctx, "singlesink-sink",
		streaming.WithSinkStartAtOldest(),
		streaming.WithSinkBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}

	// Don't forget to close the sink when done
	defer sink.Close()

	// Consume event
	event := <-sink.Subscribe()
	fmt.Printf("event: %s, payload: %s\n", event.EventName, event.Payload)

	// Acknowledge event
	if err := sink.Ack(ctx, event); err != nil {
		panic(err)
	}
}
