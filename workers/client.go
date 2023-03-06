package ponos

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	redis "github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type (
	// Pool is a pool of workers.
	Pool struct {
		Name string

		cid    string        // Client ID
		ttl    time.Duration // Client TTL
		logger ErrorHandler
		rdb    *redis.Client

		lock   sync.Mutex
		stopCh chan struct{}
	}
)

// Pool returns a client to the pool with the given name.
func Pool(name string, rdb *redis.Client, opts ...Option) (*Pool, error) {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	if options.wid == "" {
		options.wid = uuid.New().String()
	}
	return &Pool{
		Name:   name,
		cid:    options.wid,
		ttl:    options.workerTTL,
		logger: options.handler,
		rdb:    rdb,
	}, nil
}

// Enqueue adds a job to the pool.
func Enqueue[T Marshalable](ctx context.Context, p *Pool, key string, payload T) error {
	var workers []workerData
	if err := p.rdb.SMembers(ctx, p.Name).ScanSlice(&workers); err != nil {
		return err
	}
	numWorkers := p.rdb.ZCard(ctx, p.Name).Val()
	if numWorkers == 0 {
		return fmt.Errorf("no workers in pool %s", p.Name)
	}
	widx := p.h.Hash(key, numWorkers)
	p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: p.Name + ":" + strconv.Itoa(widx),
		Values: map[string]any{"payload": payload},
	})
}

// Jump Consistent Hashing, see https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
func (jh jumpHash) Hash(key string, numBuckets int64) int64 {
	var b int64 = -1
	var j int64

	jh.h.Reset()
	io.WriteString(jh.h, key)
	sum := jh.h.Sum64()

	for j < numBuckets {
		b = j
		sum = sum*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((sum>>33)+1)))
	}
	return b
}
