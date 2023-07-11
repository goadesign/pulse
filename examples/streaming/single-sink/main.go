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

func main() {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: os.Getenv("REDIS_PASSWORD")})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	// Create stream
	stream, err := streaming.NewStream("singlesink-stream", rdb)
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

	// Create sink that reads from the beginning and waits for events
	// for up to 100ms
	sink, err := stream.NewSink(ctx, "singlesink-sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}

	// Don't forget to close the sink when done
	defer sink.Close()

	// Consume event
	ev := <-sink.Subscribe()
	fmt.Printf("event: %s, payload: %s\n", ev.EventName, ev.Payload)

	// Acknowledge event
	if err := sink.Ack(ctx, ev); err != nil {
		panic(err)
	}
}
