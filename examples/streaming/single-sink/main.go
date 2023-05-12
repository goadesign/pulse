package main

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
	"goa.design/ponos/streaming"
)

func main() {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: os.Getenv("REDIS_PASSWORD")})

	// Create stream
	ctx := context.Background()
	stream, err := streaming.NewStream(ctx, "my-stream", rdb)
	if err != nil {
		panic(err)
	}

	// Add a new event
	if _, err := stream.Add(ctx, "event", []byte("payload")); err != nil {
		panic(err)
	}

	// Create sink
	sink, err := stream.NewSink(ctx, "my-sink", streaming.WithSinkStartAtOldest())
	if err != nil {
		panic(err)
	}
	defer sink.Close()

	// Consume event
	event := <-sink.C
	fmt.Printf("event: %s, payload: %s\n", event.EventName, event.Payload)

	// Acknowledge event
	if err := sink.Ack(ctx, event); err != nil {
		panic(err)
	}
}
