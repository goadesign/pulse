package main

import (
	"context"
	"fmt"
	"os"
	"time"

	redis "github.com/redis/go-redis/v9"
	"goa.design/clue/log"
	"goa.design/ponos/ponos"
	"goa.design/ponos/pool"
)

func main() {
	ctx := log.Context(context.Background(), log.WithDebug())
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: os.Getenv("REDIS_PASSWORD"),
	})

	// Create client for node named "example"
	node, err := pool.AddNode(ctx, "example", rdb,
		pool.WithClientOnly(),
		pool.WithLogger(ponos.ClueLogger(ctx)),
	)
	if err != nil {
		panic(err)
	}

	// Cleanup node on exit.
	defer func() {
		if err := node.Close(ctx); err != nil {
			panic(err)
		}
	}()

	// Start 2 jobs
	fmt.Println("** Starting job one...")
	if err := node.DispatchJob(ctx, "one", nil); err != nil {
		panic(err)
	}
	fmt.Println("** Starting job two...")
	if err := node.DispatchJob(ctx, "two", nil); err != nil {
		panic(err)
	}
	time.Sleep(200 * time.Millisecond) // emulate delay
	fmt.Println("Stopping job one...")
	if err := node.StopJob(ctx, "one"); err != nil {
		panic(err)
	}
	fmt.Println("Notifying worker for job two...")
	if err := node.NotifyWorker(ctx, "two", []byte("hello")); err != nil {
		panic(err)
	}
	fmt.Println("Stopping job two...")
	if err := node.StopJob(ctx, "two"); err != nil {
		panic(err)
	}
}
