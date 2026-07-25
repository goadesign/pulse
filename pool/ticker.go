package pool

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"goa.design/pulse/pulse"
	"goa.design/pulse/rmap"
)

type (
	// Ticker represents a clock that periodically sends ticks to one of the pool nodes
	// which created a ticker with the same name.
	Ticker struct {
		C         <-chan time.Time
		c         chan time.Time
		name      string
		node      *Node
		lock      sync.Mutex
		tickerMap *rmap.Map
		timer     *time.Timer
		next      string // serialized next tick time in unix micro and duration
		mapch     <-chan rmap.EventKind
		wg        *sync.WaitGroup
		logger    pulse.Logger
	}
)

const (
	// tickerRetryMaxInterval caps how long a local ticker waits before retrying
	// a failed Redis advance. A transient Redis outage must not leave the
	// distributed ticker permanently idle until some unrelated map event arrives.
	tickerRetryMaxInterval = time.Second
)

// NewTicker returns a new Ticker that behaves similarly to time.Ticker, but
// instead delivers the current time on the channel to only one of the nodes
// that invoked NewTicker with the same name.
func (node *Node) NewTicker(ctx context.Context, name string, d time.Duration, opts ...TickerOption) (*Ticker, error) {
	if node.clientOnly {
		return nil, fmt.Errorf("cannot create ticker on client-only node")
	}
	if err := node.ensureGenerationActive(ctx); err != nil {
		return nil, fmt.Errorf("create ticker: %w", err)
	}
	if d < time.Millisecond {
		return nil, fmt.Errorf("create ticker: duration must be at least 1ms")
	}
	name = node.PoolName + ":" + name
	o := parseTickerOptions(opts...)
	logger := o.logger
	if logger == nil {
		logger = pulse.NoopLogger()
	}
	c := make(chan time.Time)
	t := &Ticker{
		C:         c,
		c:         c,
		name:      name,
		node:      node,
		tickerMap: node.tickerMap,
		mapch:     node.tickerMap.Subscribe(),
		wg:        &sync.WaitGroup{},
		logger:    logger,
	}
	if current, ok := node.tickerMap.Get(name); ok {
		_, curd, err := deserialize(current)
		if err != nil {
			node.tickerMap.Unsubscribe(t.mapch)
			return nil, fmt.Errorf("create ticker: decode shared state: %w", err)
		}
		if d == curd {
			t.next = current
		}
	}
	if t.next == "" {
		next := serialize(time.Now().Add(d), d)
		if err := node.setPoolMapAndWait(ctx, node.tickerMap, node.resources.tickers, t.name, next); err != nil {
			return nil, fmt.Errorf("failed to store tick and duration: %s", err)
		}
		t.next = next
	}
	if err := t.initTimer(); err != nil {
		node.tickerMap.Unsubscribe(t.mapch)
		return nil, fmt.Errorf("create ticker: %w", err)
	}
	t.wg.Add(1)
	pulse.Go(logger, func() { t.handleEvents() })
	return t, nil
}

// Close stops the ticker locally.
//
// Close does not delete the shared ticker-map entry. Use Close when a node wants
// to stop processing ticks without affecting other nodes that may be
// participating in the same distributed ticker.
//
// Close does not close the tick channel to avoid racing with concurrent
// receivers (matching time.Ticker semantics).
func (t *Ticker) Close() {
	t.lock.Lock()
	if t.timer != nil {
		t.timer.Stop()
	}
	if t.mapch != nil {
		t.tickerMap.Unsubscribe(t.mapch)
	}
	t.mapch = nil
	t.lock.Unlock()
	t.wg.Wait()
}

// Stop turns off a ticker. After Stop, no more ticks will be sent. Stop does
// not close the channel, to prevent a concurrent goroutine reading from the
// channel from seeing an erroneous "tick".
func (t *Ticker) Stop() {
	if err := t.stop(context.Background()); err != nil {
		t.logger.Error(err, "msg", "failed to stop ticker")
	}
}

// stop deletes the canonical shared ticker before stopping this local replica.
// A deletion failure leaves the ticker live so its owner can retry.
func (t *Ticker) stop(ctx context.Context) error {
	t.lock.Lock()
	if err := t.node.deletePoolMap(ctx, t.node.resources.tickers, t.name); err != nil {
		t.lock.Unlock()
		return fmt.Errorf("delete shared ticker %q: %w", t.name, err)
	}
	if t.timer != nil {
		t.timer.Stop()
	}
	if t.mapch != nil {
		t.tickerMap.Unsubscribe(t.mapch)
	}
	t.mapch = nil
	t.lock.Unlock()
	t.wg.Wait()
	return nil
}

// handleEvents handles events from the ticker timer and map.
func (t *Ticker) handleEvents() {
	defer t.wg.Done()
	t.lock.Lock()
	if t.mapch == nil {
		t.lock.Unlock()
		return
	}
	ch := t.mapch
	t.lock.Unlock()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				t.logger.Info("stopped locally")
				return
			}
			next, ok := t.tickerMap.Get(t.name)
			if !ok {
				t.logger.Info("stopped remotely")
				t.lock.Lock()
				if t.mapch != nil {
					t.tickerMap.Unsubscribe(t.mapch)
				}
				t.mapch = nil
				t.lock.Unlock()
				return
			}
			t.lock.Lock()
			if next == t.next {
				// No change.
				t.lock.Unlock()
				continue
			}
			t.next = next
			if err := t.initTimer(); err != nil {
				t.logger.Error(err, "msg", "invalid shared ticker state")
				t.stopInvalidStateLocked()
				t.lock.Unlock()
				return
			}
			t.lock.Unlock()
		case <-t.timer.C:
			t.handleTick()
		}
	}
}

// handleTick sends the current time on the channel.
func (t *Ticker) handleTick() {
	t.lock.Lock()
	defer t.lock.Unlock()
	ts, d, err := deserialize(t.next)
	if err != nil {
		t.logger.Error(err, "msg", "invalid shared ticker state")
		t.stopInvalidStateLocked()
		return
	}
	ts = ts.Add(d)
	for ts.Before(time.Now()) {
		ts = ts.Add(d)
	}
	next := serialize(ts, d)
	prev, err := t.node.testAndSetPoolMap(
		context.Background(),
		t.node.resources.tickers,
		t.name,
		t.next,
		next,
	)
	if err != nil {
		t.handleAdvanceFailureLocked(err, d)
		return
	}
	if prev != t.next {
		// Another node already updated the ticker, restart the timer.
		t.next = prev
		if err := t.initTimer(); err != nil {
			t.logger.Error(err, "msg", "invalid shared ticker state")
			t.stopInvalidStateLocked()
		}
		return
	}
	t.next = next
	if err := t.initTimer(); err != nil {
		t.logger.Error(err, "msg", "invalid shared ticker state")
		t.stopInvalidStateLocked()
		return
	}
	select {
	case t.c <- time.Now():
	default:
	}
}

// initTimer sets the timer to fire at the next strictly decoded tick.
func (t *Ticker) initTimer() error {
	next, _, err := deserialize(t.next)
	if err != nil {
		return err
	}
	t.resetTimerLocked(time.Until(next))
	return nil
}

// stopInvalidStateLocked stops this replica after Redis returned malformed
// canonical state. The caller holds lock; another explicit NewTicker may repair
// the state only through the normal construction contract.
func (t *Ticker) stopInvalidStateLocked() {
	if t.mapch != nil {
		t.tickerMap.Unsubscribe(t.mapch)
		t.mapch = nil
	}
	if t.timer != nil {
		t.timer.Stop()
	}
}

// handleAdvanceFailureLocked logs a transient Redis advance failure and rearms
// the timer so the ticker retries instead of staying permanently idle. The
// caller must hold t.lock.
func (t *Ticker) handleAdvanceFailureLocked(err error, interval time.Duration) {
	t.logger.Error(err, "msg", "failed to update next tick")
	if errors.Is(err, ErrPoolGenerationLost) {
		if t.mapch != nil {
			t.tickerMap.Unsubscribe(t.mapch)
			t.mapch = nil
		}
		t.timer.Stop()
		return
	}
	t.resetTimerLocked(min(interval, tickerRetryMaxInterval))
}

// resetTimerLocked arms the local timer to fire after d. The caller must hold
// t.lock.
func (t *Ticker) resetTimerLocked(d time.Duration) {
	if d < 0 {
		d = 0
	}
	if t.timer == nil {
		t.timer = time.NewTimer(d)
		return
	}
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
	t.timer.Reset(d)
}

// serialize returns a serialized representation of the given time and duration.
func serialize(t time.Time, d time.Duration) string {
	ts := strconv.FormatInt(t.UnixMicro(), 10)
	ds := d.String()
	return ts + "|" + ds
}

// deserialize validates and returns one canonical shared ticker state.
func deserialize(s string) (time.Time, time.Duration, error) {
	parts := strings.Split(s, "|")
	if len(parts) != 2 {
		return time.Time{}, 0, fmt.Errorf("ticker state %q must contain timestamp and duration", s)
	}
	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("ticker state %q has invalid timestamp: %w", s, err)
	}
	d, err := time.ParseDuration(parts[1])
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("ticker state %q has invalid duration: %w", s, err)
	}
	if d < time.Millisecond {
		return time.Time{}, 0, fmt.Errorf("ticker state %q has duration below 1ms", s)
	}
	return time.UnixMicro(ts), d, nil
}
