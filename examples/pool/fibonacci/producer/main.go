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

	// Get pool named "fibonacci"
	pool, err := pool.AddNode(ctx, "fibonacci", rdb, pool.WithLogger(ponos.ClueLogger(ctx)))
	if err != nil {
		panic(err)
	}

	// Queue new job with key "key" and payload 42
	var n = uint64(42)
	if len(os.Args) > 1 {
		fmt.Sscanf(os.Args[1], "%d", &n)
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, n)
	fmt.Println("Dispatching job...")
	if err := pool.DispatchJob(ctx, fmt.Sprintf("%d", n), payload); err != nil {
		panic(err)
	}
}
