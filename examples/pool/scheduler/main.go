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

	// Create node for pool "example".
	node, err := pool.AddNode(ctx, "example", rdb, pool.WithLogger(logger))
	if err != nil {
		panic(err)
	}

	// Start schedule
	log.Infof(ctx, "Starting schedule... CTRL+C to stop.")
	done := make(chan struct{})
	if err := node.Schedule(ctx, newProducer(ctx, done), time.Second); err != nil {
		panic(err)
	}

	// Wait for producer to be done
	<-done

	// Cleanup node on exit.
	if err := node.Close(ctx); err != nil {
		panic(err)
	}
}

// producer is a job producer that alternatively starts and stops a job.
// It closes the done channel when it is done.
type producer struct {
	iter   int
	done   chan struct{}
	logctx context.Context
}

func newProducer(ctx context.Context, done chan struct{}) *producer {
	return &producer{done: done, logctx: ctx}
}

func (p *producer) Name() string {
	return "example"
}

// Plan is called by the scheduler to determine the next job to start or stop.
func (p *producer) Plan() (*pool.JobPlan, error) {
	p.iter++
	if p.iter > 10 {
		log.Infof(p.logctx, "done")
		close(p.done)
		return nil, pool.ErrScheduleStop
	}
	if p.iter%2 == 0 {
		log.Infof(p.logctx, "stop all")
		return &pool.JobPlan{StopAll: true}, nil
	}
	log.Infof(p.logctx, "start job")
	return &pool.JobPlan{Start: []*pool.JobParam{{Key: "job", Payload: []byte("payload")}}}, nil
}
