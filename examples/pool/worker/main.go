package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	// Connect to or create pool "".
	pool, err := pool.AddNode(ctx, "example", rdb, pool.WithLogger(ponos.ClueLogger(ctx)))
	if err != nil {
		panic(err)
	}

	// Create a new worker for pool "example".
	handler := &JobHandler{executions: make(map[string]*Execution)}
	if _, err := pool.AddWorker(ctx, handler); err != nil {
		panic(err)
	}

	// Block until a signal is received
	fmt.Println("Waiting for jobs...")
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	<-interrupt
	fmt.Println("\nCtrl+C signal received. Exiting...")
	if err := pool.Shutdown(ctx); err != nil {
		panic(err)
	}
	os.Exit(0)
}
