package main

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
	"goa.design/clue/log"
	"goa.design/ponos/ponos"
	"goa.design/ponos/streaming"
)

func main() {
	// Create a Redis client
	pwd := os.Getenv("REDIS_PASSWORD")
	if pwd == "" {
		panic("REDIS_PASSWORD not set")
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: pwd,
	})
	ctx := log.Context(context.Background())
	log.FlushAndDisableBuffering(ctx)

	// Create example stream
	stream, err := streaming.NewStream(ctx, "my-stream", rdb, streaming.WithLogger(ponos.ClueLogger(ctx)))
	if err != nil {
		panic(err)
	}

	// Add a new event
	if err := stream.Add(context.Background(), "event", []byte("payload")); err != nil {
		panic(err)
	}

	// Create event sink
	sink, err := stream.NewSink(ctx, "my-sink")
	if err != nil {
		panic(err)
	}

	// Consume all events
	for event := range sink.C {
		fmt.Printf("event: %s, payload: %s\n", event.EventName, event.Payload)
		event.Ack(ctx)
	}
}
