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

// NOTE: the example below does not handle errors for brevity.
func main() {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: os.Getenv("REDIS_PASSWORD")})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	// Create stream
	stream, err := streaming.NewStream("pubsub-stream", rdb)
	if err != nil {
		panic(err)
	}

	// Don't forget to destroy the stream when done
	defer stream.Destroy(ctx)

	// Add a new event to topic "my-topic"
	id1, err := stream.Add(ctx,
		"event 1",
		[]byte("payload 1"),
		options.WithTopic("my-topic"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("event 1 id: %s\n", id1)

	// Add a new event to topic "other-topic"
	id2, err := stream.Add(ctx, "event 2", []byte("payload 2"), options.WithTopic("other-topic"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("event 2 id: %s\n", id2)

	// Create sink that reads from the beginning and waits for events for up
	// to 100ms
	sink, err := stream.NewSink(ctx, "pubsub-sink",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}

	// Don't forget to close the sink when done
	defer sink.Close()

	// Read both events
	c := sink.Subscribe()
	event := <-c
	fmt.Printf("topic: %s, event: %s, payload: %s\n", event.Topic, event.EventName, event.Payload)
	if err := sink.Ack(ctx, event); err != nil {
		panic(err)
	}

	event = <-c
	fmt.Printf("topic: %s, event: %s, payload: %s\n", event.Topic, event.EventName, event.Payload)
	if err := sink.Ack(ctx, event); err != nil {
		panic(err)
	}

	// Create reader that reads from the beginning, waits for events for up
	// to 100ms and only reads events whose topic match the pattern "my-*"
	reader, err := stream.NewReader(ctx,
		options.WithReaderStartAtOldest(),
		options.WithReaderBlockDuration(100*time.Millisecond),
		options.WithReaderTopicPattern("my-*"))
	if err != nil {
		panic(err)
	}

	// Don't forget to close the reader when done
	defer reader.Close()

	// Read event from topic "my-topic"
	event = <-reader.Subscribe()
	fmt.Printf("reader topic pattern: my-*, topic: %s, event: %s, payload: %s\n", event.Topic, event.EventName, event.Payload)
}
