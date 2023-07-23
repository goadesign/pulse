package main

import (
	"context"
	"os"
	"time"

	redis "github.com/redis/go-redis/v9"
	"goa.design/clue/log"
	"goa.design/pulse/pool"
	"goa.design/pulse/pulse"
)

func main() {
	// Setup Redis connection
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: os.Getenv("REDIS_PASSWORD"),
	})

	// Setup clue logger.
	ctx := log.Context(context.Background())
	log.FlushAndDisableBuffering(ctx)

	var logger pulse.Logger
	if len(os.Args) > 1 && os.Args[1] == "-v" {
		logger = pulse.ClueLogger(ctx)
	}

	// Create client for worker pool "example"
	client, err := pool.AddNode(ctx, "example", rdb,
		pool.WithClientOnly(),
		pool.WithLogger(logger),
	)
	if err != nil {
		panic(err)
	}

	// Start 2 jobs
	log.Infof(ctx, "starting job one")
	if err := client.DispatchJob(ctx, "alpha", nil); err != nil {
		panic(err)
	}
	log.Infof(ctx, "starting job two")
	if err := client.DispatchJob(ctx, "beta", nil); err != nil {
		panic(err)
	}
	time.Sleep(200 * time.Millisecond) // emulate delay

	// Stop job one
	log.Infof(ctx, "stopping job one")
	if err := client.StopJob(ctx, "alpha"); err != nil {
		panic(err)
	}

	// Notify and then stop job two
	log.Infof(ctx, "notifying job two worker")
	if err := client.NotifyWorker(ctx, "beta", []byte("hello")); err != nil {
		panic(err)
	}
	log.Infof(ctx, "stopping job two")
	if err := client.StopJob(ctx, "beta"); err != nil {
		panic(err)
	}

	// Cleanup client on exit.
	log.Infof(ctx, "done")
	if err := client.Close(ctx); err != nil {
		panic(err)
	}
}
