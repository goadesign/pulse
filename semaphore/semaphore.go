// Package semaphore provides Redis-backed leased semaphores for bounding
// concurrent work across a fleet. It is intentionally narrower than Pulse pool:
// callers synchronously wait for capacity, receive a lease token when admitted,
// and release that token when the protected work finishes.
package semaphore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"goa.design/pulse/pulse"
)

type (
	// Semaphore coordinates named, leased concurrency slots through Redis.
	Semaphore struct {
		rdb             *redis.Client
		name            string
		limit           int
		holders         string
		waiters         string
		waiterDeadlines string
		waiterSeq       string
		channel         string
		leaseTTL        time.Duration
		waiterTTL       time.Duration
		heartbeat       time.Duration
		logger          pulse.Logger
	}

	// Lease is one admitted semaphore slot. Release must be called when the
	// protected work completes; if the process dies, the lease expires after the
	// semaphore's TTL.
	Lease struct {
		sem      *Semaphore
		token    string
		ctx      context.Context
		cancel   context.CancelFunc
		stop     chan struct{}
		stopOnce sync.Once
		mu       sync.Mutex
		released bool
		err      error
	}

	// Option configures a Semaphore.
	Option func(*options)

	options struct {
		leaseTTL  time.Duration
		heartbeat time.Duration
		logger    pulse.Logger
	}
)

const (
	defaultLeaseTTL  = 30 * time.Second
	defaultHeartbeat = 10 * time.Second
)

var (
	// ErrLimitRequired reports an invalid acquire limit.
	ErrLimitRequired = errors.New("semaphore limit must be positive")

	// ErrLeaseLost reports that Redis no longer recognizes the holder token.
	ErrLeaseLost = errors.New("semaphore lease lost")
)

// New creates a named semaphore backed by Redis. All processes that use the same
// name share the same holder set and wakeup channel.
func New(rdb *redis.Client, name string, limit int, opts ...Option) (*Semaphore, error) {
	if rdb == nil {
		return nil, errors.New("semaphore Redis client is required")
	}
	if name == "" {
		return nil, errors.New("semaphore name is required")
	}
	if limit <= 0 {
		return nil, ErrLimitRequired
	}
	cfg, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	return &Semaphore{
		rdb:             rdb,
		name:            name,
		limit:           limit,
		holders:         fmt.Sprintf("semaphore:%s:holders", name),
		waiters:         fmt.Sprintf("semaphore:%s:waiters", name),
		waiterDeadlines: fmt.Sprintf("semaphore:%s:waiter-deadlines", name),
		waiterSeq:       fmt.Sprintf("semaphore:%s:waiter-seq", name),
		channel:         fmt.Sprintf("semaphore:%s:wake", name),
		leaseTTL:        cfg.leaseTTL,
		waiterTTL:       cfg.leaseTTL * 2,
		heartbeat:       cfg.heartbeat,
		logger:          cfg.logger,
	}, nil
}

// WithLeaseTTL sets the maximum time a holder may keep a slot without a
// heartbeat. It is the crash-recovery bound for leaked slots.
func WithLeaseTTL(ttl time.Duration) Option {
	return func(o *options) {
		o.leaseTTL = ttl
	}
}

// WithHeartbeatInterval sets how often an active lease renews its expiry.
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(o *options) {
		o.heartbeat = interval
	}
}

// WithLogger sets the logger used for asynchronous heartbeat failures.
func WithLogger(logger pulse.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

// Acquire blocks until a slot is available or ctx is canceled.
func (s *Semaphore) Acquire(ctx context.Context) (*Lease, error) {
	token := uuid.NewString()
	lease, wait, err := s.tryAcquire(ctx, token)
	if err != nil {
		return nil, err
	}
	if lease != nil {
		return lease, nil
	}
	sub := s.rdb.Subscribe(ctx, s.channel)
	if _, err := sub.Receive(ctx); err != nil {
		s.cancelWaiter(context.Background(), token)
		return nil, fmt.Errorf("subscribe semaphore %q: %w", s.name, err)
	}
	defer func() {
		s.cancelWaiter(context.Background(), token)
		if err := sub.Close(); err != nil {
			s.logger.Error(err, "msg", "close semaphore subscription", "name", s.name)
		}
	}()
	events := sub.Channel()
	for {
		lease, nextWait, err := s.tryAcquire(ctx, token)
		if err != nil {
			return nil, err
		}
		if lease != nil {
			return lease, nil
		}
		wait = nextWait
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return nil, ctx.Err()
		case <-timer.C:
		case _, ok := <-events:
			stopTimer(timer)
			if !ok {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return nil, errors.New("semaphore subscription closed")
			}
		}
	}
}

// Release frees the lease's slot and wakes other acquirers. It is idempotent for
// a lease that has already released successfully.
func (l *Lease) Release(ctx context.Context) error {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	leaseErr := l.err
	l.mu.Unlock()

	removed, err := l.sem.release(ctx, l.token)
	if err != nil {
		return err
	}
	if !removed && leaseErr == nil {
		leaseErr = ErrLeaseLost
		l.markLost(leaseErr)
	}
	l.markReleased()
	if leaseErr != nil {
		return leaseErr
	}
	return err
}

// Context is canceled when the caller's acquire context is canceled, Release is
// called, or the semaphore can no longer renew the Redis lease.
func (l *Lease) Context() context.Context {
	return l.ctx
}

// Token returns the holder token stored in Redis. It is useful for diagnostics.
func (l *Lease) Token() string {
	return l.token
}

// Err returns the terminal lease error, if lease liveness was lost.
func (l *Lease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// tryAcquire removes expired holders, attempts to insert a fresh holder token,
// and returns the Redis-computed wait duration when the semaphore is full.
func (s *Semaphore) tryAcquire(ctx context.Context, token string) (*Lease, time.Duration, error) {
	res, err := luaAcquire.Run(
		ctx,
		s.rdb,
		[]string{s.holders, s.channel, s.waiters, s.waiterDeadlines, s.waiterSeq},
		token,
		s.limit,
		s.leaseTTL.Milliseconds(),
		s.waiterTTL.Milliseconds(),
	).Slice()
	if err != nil {
		return nil, 0, fmt.Errorf("acquire semaphore %q: %w", s.name, err)
	}
	acquired, err := intResult(res[0])
	if err != nil {
		return nil, 0, fmt.Errorf("decode semaphore %q acquire result: %w", s.name, err)
	}
	waitMillis, err := intResult(res[1])
	if err != nil {
		return nil, 0, fmt.Errorf("decode semaphore %q wait result: %w", s.name, err)
	}
	if acquired == 1 {
		leaseCtx, cancel := context.WithCancel(ctx)
		lease := &Lease{
			sem:    s,
			token:  token,
			ctx:    leaseCtx,
			cancel: cancel,
			stop:   make(chan struct{}),
		}
		go lease.heartbeatLoop()
		return lease, 0, nil
	}
	return nil, max(time.Duration(waitMillis)*time.Millisecond, time.Millisecond), nil
}

// release removes the holder token if it still exists and wakes blocked
// acquirers.
func (s *Semaphore) release(ctx context.Context, token string) (bool, error) {
	removed, err := luaRelease.Run(ctx, s.rdb, []string{s.holders, s.channel, s.waiters, s.waiterDeadlines, s.waiterSeq}, token).Int64()
	if err != nil {
		return false, fmt.Errorf("release semaphore %q: %w", s.name, err)
	}
	return removed == 1, nil
}

// cancelWaiter removes a queued acquire token when its caller stops waiting.
func (s *Semaphore) cancelWaiter(ctx context.Context, token string) {
	if err := luaCancelWaiter.Run(ctx, s.rdb, []string{s.waiters, s.waiterDeadlines, s.channel, s.holders, s.waiterSeq}, token).Err(); err != nil {
		s.logger.Error(err, "msg", "cancel semaphore waiter", "name", s.name)
	}
}

// heartbeatLoop keeps the lease alive until Release stops it or Redis reports
// that the token no longer exists.
func (l *Lease) heartbeatLoop() {
	ticker := time.NewTicker(l.sem.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			if err := l.renew(context.Background()); err != nil {
				l.markLost(err)
				l.sem.logger.Error(err, "msg", "renew semaphore lease", "name", l.sem.name)
				return
			}
		}
	}
}

// renew extends the holder score for an active lease token.
func (l *Lease) renew(ctx context.Context) error {
	ok, err := luaRenew.Run(ctx, l.sem.rdb, []string{l.sem.holders, l.sem.channel}, l.token, l.sem.leaseTTL.Milliseconds()).Int64()
	if err != nil {
		return fmt.Errorf("renew semaphore %q: %w", l.sem.name, err)
	}
	if ok == 0 {
		return fmt.Errorf("semaphore %q lease is no longer held: %w", l.sem.name, ErrLeaseLost)
	}
	return nil
}

// markLost records a lease-liveness failure and cancels the lease context.
func (l *Lease) markLost(err error) {
	l.mu.Lock()
	if l.err == nil {
		l.err = err
	}
	l.mu.Unlock()
	l.cancel()
	l.stopOnce.Do(func() { close(l.stop) })
}

// markReleased records successful release, stops heartbeat, and cancels the
// lease context because the protected work no longer owns capacity.
func (l *Lease) markReleased() {
	l.mu.Lock()
	l.released = true
	l.mu.Unlock()
	l.cancel()
	l.stopOnce.Do(func() { close(l.stop) })
}

// parseOptions applies defaults and keeps heartbeat strictly shorter than the
// lease TTL so active holders renew before their crash-recovery deadline.
func parseOptions(opts ...Option) (*options, error) {
	o := &options{
		leaseTTL:  defaultLeaseTTL,
		heartbeat: defaultHeartbeat,
		logger:    pulse.NoopLogger(),
	}
	for _, opt := range opts {
		opt(o)
	}
	if o.leaseTTL <= 0 {
		return nil, errors.New("semaphore lease TTL must be positive")
	}
	if o.heartbeat <= 0 {
		return nil, errors.New("semaphore heartbeat interval must be positive")
	}
	if o.heartbeat >= o.leaseTTL {
		return nil, errors.New("semaphore heartbeat interval must be shorter than lease TTL")
	}
	return o, nil
}

// intResult decodes Redis Lua integer and bulk-string numeric results.
func intResult(value any) (int, error) {
	switch v := value.(type) {
	case int64:
		return int(v), nil
	case string:
		out, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		return out, nil
	default:
		return 0, fmt.Errorf("unexpected integer result %T", value)
	}
}

// stopTimer drains a timer that may already have fired before a pub/sub wakeup
// or context cancellation won the acquire loop select.
func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
