package pool

import (
	"context"
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
		lock      sync.Mutex
		tickerMap *rmap.Map
		timer     *time.Timer
		next      string // serialized next tick time in unix micro and duration
		mapch     <-chan rmap.EventKind
		wg        *sync.WaitGroup
		logger    pulse.Logger
	}
)

// NewTicker returns a new Ticker that behaves similarly to time.Ticker, but
// instead delivers the current time on the channel to only one of the nodes
// that invoked NewTicker with the same name.
func (node *Node) NewTicker(ctx context.Context, name string, d time.Duration, opts ...TickerOption) (*Ticker, error) {
	if node.clientOnly {
		return nil, fmt.Errorf("cannot create ticker on client-only node")
	}
	name = node.Name + ":" + name
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
		tickerMap: node.tickerMap,
		mapch:     node.tickerMap.Subscribe(),
		wg:        &sync.WaitGroup{},
		logger:    logger,
	}
	if current, ok := node.tickerMap.Get(name); ok {
		t.next = current
	} else {
		next := serialize(time.Now().Add(d), d)
		if _, err := t.tickerMap.Set(ctx, t.name, next); err != nil {
			return nil, fmt.Errorf("failed to store tick and duration: %s", err)
		}
		t.next = next
	}
	t.initTimer()
	t.wg.Add(1)
	go t.handleEvents()
	return t, nil
}

// Reset stops a ticker and resets its period to the specified duration. The
// next tick will arrive after the new period elapses. The duration d must be
// greater than zero; if not, Reset will panic.
func (t *Ticker) Reset(d time.Duration) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.next = serialize(time.Now().Add(d), d)
	_, err := t.tickerMap.Set(context.Background(), t.name, t.next)
	if err != nil {
		t.logger.Error(err, "msg", "failed to reset ticker")
		return
	}
	t.initTimer()
	if t.mapch == nil {
		// ticker was previously stopped, restart it.
		t.mapch = t.tickerMap.Subscribe()
		t.wg.Add(1)
		go t.handleEvents()
	}
}

// Stop turns off a ticker. After Stop, no more ticks will be sent. Stop does
// not close the channel, to prevent a concurrent goroutine reading from the
// channel from seeing an erroneous "tick".
func (t *Ticker) Stop() {
	t.lock.Lock()
	t.timer.Stop()
	t.tickerMap.Delete(context.Background(), t.name)
	t.tickerMap.Unsubscribe(t.mapch)
	t.mapch = nil
	t.lock.Unlock()
	t.wg.Wait()
}

// handleEvents handles events from the ticker timer and map.
func (t *Ticker) handleEvents() {
	defer t.wg.Done()
	ch := t.mapch
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
				t.tickerMap.Unsubscribe(t.mapch)
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
			t.initTimer()
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
	ts, d := deserialize(t.next)
	ts = ts.Add(d)
	for ts.Before(time.Now()) {
		ts = ts.Add(d)
	}
	next := serialize(ts, d)
	prev, err := t.tickerMap.TestAndSet(context.Background(), t.name, t.next, next)
	if err != nil {
		t.logger.Error(err, "msg", "failed to update next tick")
		return
	}
	if prev != t.next {
		// Another node already updated the ticker, restart the timer.
		t.next = prev
		t.initTimer()
		return
	}
	t.next = next
	t.initTimer()
	select {
	case t.c <- time.Now():
	default:
	}
}

// initTimer sets the timer to fire at the next tick.
func (t *Ticker) initTimer() {
	next, _ := deserialize(t.next)
	d := next.Sub(time.Now())
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

// deserialize returns the time and duration represented by the given serialized
// string. s must be a value returned by serialize, the behavior is undefined
// otherwise.
func deserialize(s string) (time.Time, time.Duration) {
	parts := strings.Split(s, "|")
	ts, _ := strconv.ParseInt(parts[0], 10, 64)
	t := time.UnixMicro(ts)
	d, _ := time.ParseDuration(parts[1])
	return t, d
}
