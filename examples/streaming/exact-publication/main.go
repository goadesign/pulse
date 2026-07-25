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
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Password: os.Getenv("REDIS_PASSWORD"),
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	deadline := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	stream, err := streaming.NewStream(
		"exact-publication",
		rdb,
		options.WithStreamDeadline(deadline),
		options.WithStreamMaxLen(1_000),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := stream.Destroy(ctx); err != nil {
			panic(err)
		}
	}()

	first, err := stream.AddOnce(
		ctx,
		"facility-42:alarm-7",
		"alarm-opened",
		[]byte("high discharge pressure"),
		options.WithTopic("alarms"),
	)
	if err != nil {
		panic(err)
	}
	retry, err := stream.AddOnce(
		ctx,
		"facility-42:alarm-7",
		"alarm-opened",
		[]byte("high discharge pressure"),
		options.WithTopic("alarms"),
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("first=%s retry=%s\n", first, retry)

	events, err := stream.Snapshot(ctx)
	if err != nil {
		panic(err)
	}
	for _, event := range events {
		fmt.Printf("%s %s: %s\n", event.ID(), event.EventName(), event.Payload())
	}
}
