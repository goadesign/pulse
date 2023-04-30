package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

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

	// Connect to or create pool "fibonacci".
	pool, err := pool.Pool(ctx, "fibonacci", rdb, pool.WithLogger(ponos.ClueLogger(ctx)))
	if err != nil {
		panic(err)
	}

	// Create a new worker for pool "fibonacci".
	worker, err := pool.NewWorker(ctx)
	if err != nil {
		panic(err)
	}

	// Handle jobs
	fmt.Println("Waiting for jobs...")
	for job := range worker.C {
		n := binary.BigEndian.Uint64(job.Payload)
		fmt.Printf(">> fib(%d)=%d\n", n, fib(n))
	}
}

func fib(n uint64) uint64 {
	if n <= 1 {
		return n
	}
	return fib(n-1) + fib(n-2)
}
