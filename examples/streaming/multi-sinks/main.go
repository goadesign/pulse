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
	rdb := redis.NewClient(&redis.Options{Addr: os.Getenv("REDIS_ADDR"), Password: os.Getenv("REDIS_PASSWORD")})
	ctx := context.Background()

	// Make sure Redis is up and running and we can connect to it
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	// Create stream
	stream, err := streaming.NewStream("multisinks", rdb)
	if err != nil {
		panic(err)
	}

	// Don't forget to destroy the stream when done
	defer func() {
		if err := stream.Destroy(ctx); err != nil {
			panic(err)
		}
	}()

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

	// Create sink that reads from the beginning and waits for events for up
	// to 100ms
	sink1, err := stream.NewSink(ctx, "multisinks-sink1",
		options.WithSinkStartAtOldest(),
		options.WithSinkBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}

	// Don't forget to close the sink when done
	defer func() {
		if err := sink1.Close(ctx); err != nil {
			panic(err)
		}
	}()

	// Read and acknowlege event
	ev := <-sink1.Subscribe()
	fmt.Printf("sink 1, event: %s, payload: %s\n", ev.EventName, ev.Payload)
	if err := sink1.Ack(ctx, ev); err != nil {
		panic(err)
	}

	// Create sink and start reading after first event
	sink2, err := stream.NewSink(ctx, "multisinks-sink2",
		options.WithSinkStartAfter(ev.ID),
		options.WithSinkBlockDuration(100*time.Millisecond))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := sink2.Close(ctx); err != nil {
			panic(err)
		}
	}()

	// Read second event
	ev = <-sink2.Subscribe()
	fmt.Printf("sink 2, event: %s, payload: %s\n", ev.EventName, ev.Payload)
	if err := sink2.Ack(ctx, ev); err != nil {
		panic(err)
	}
}
