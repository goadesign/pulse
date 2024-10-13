package pool

import (
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"goa.design/pulse/rmap"
	ptesting "goa.design/pulse/testing"
)

func TestSchedule(t *testing.T) {
	var (
		rdb      = ptesting.NewRedisClient(t)
		ctx, buf = ptesting.NewBufferedLogContext(t)
		testName = ulid.Make().String()
		node     = newTestNode(t, ctx, rdb, testName)
		worker   = newTestWorker(t, ctx, node)
		d        = 10 * time.Millisecond
		iter     = 0
		lock     sync.Mutex
	)
	defer ptesting.CleanupRedis(t, rdb, false, testName)

	inc := func() { lock.Lock(); iter++; lock.Unlock() }
	it := func() int { lock.Lock(); defer lock.Unlock(); return iter }

	producer := newTestProducer(testName, func() (*JobPlan, error) {
		inc()
		switch it() {
		case 1:
			assert.Equal(t, 0, numJobs(t, worker), "unexpected number of jobs")
			// First iteration: start a job
			return &JobPlan{Start: []*JobParam{{Key: testName, Payload: []byte("payload")}}}, nil
		case 2:
			assert.Eventually(t, func() bool { return numJobs(t, worker) == 1 }, max, delay, "job not started")
			// Second iteration: stop job
			return &JobPlan{Stop: []string{testName}}, nil
		case 3:
			assert.Eventually(t, func() bool { return numJobs(t, worker) == 0 }, max, delay, "job not stopped")
			// Third iteration: start two jobs
			return &JobPlan{Start: []*JobParam{
				{Key: testName + "1", Payload: []byte("payload")},
				{Key: testName + "2", Payload: []byte("payload")}}}, nil
		case 4:
			assert.Eventually(t, func() bool { return numJobs(t, worker) == 2 }, max, delay, "jobs not started")
			// Fourth iteration: stop all jobs
			return &JobPlan{StopAll: true}, nil
		case 5:
			assert.Eventually(t, func() bool { return numJobs(t, worker) == 0 }, max, delay, "jobs not stopped")
			// Fifth iteration: start one
			return &JobPlan{Start: []*JobParam{{Key: testName, Payload: []byte("payload")}}}, nil
		case 6:
			assert.Eventually(t, func() bool { return numJobs(t, worker) == 1 }, max, delay, "job not started")
			//  Sixth iteration: start one, stop one
			return &JobPlan{
				Start:   []*JobParam{{Key: testName + "1", Payload: []byte("payload")}},
				StopAll: true,
			}, nil
		case 7:
			assert.Eventually(t, func() bool { return numJobs(t, worker) == 1 }, max, delay, "job not started")
			// Seventh iteration: stop schedule
			return nil, ErrScheduleStop
		}
		t.Errorf("unexpected iteration %d", it())
		return nil, nil
	})

	// Observe call to reset
	jobMap, err := rmap.Join(ctx, testName+":"+testName, rdb)
	require.NoError(t, err)
	var reset bool
	c := jobMap.Subscribe()
	defer jobMap.Unsubscribe(c)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range c {
			if ev == rmap.EventReset {
				reset = true
				return
			}
		}
	}()

	err = node.Schedule(ctx, producer, d)
	require.NoError(t, err)

	assert.Eventually(t, func() bool { return it() == 7 }, max, delay, "schedule should have stopped")
	select {
	case <-done:
		reset = true
	case <-time.After(time.Second):
		break
	}
	assert.True(t, reset, "job map should have been reset")
	assert.NotContains(t, buf.String(), "level=error", "unexpected logged error")
}

type testProducer struct {
	name    string
	compute func() (*JobPlan, error)
}

// newTestProducer returns a producer with the given name and compute schedule
// function.
func newTestProducer(name string, compute func() (*JobPlan, error)) JobProducer {
	return &testProducer{name: name, compute: compute}
}
func (p *testProducer) Name() string            { return p.name }
func (p *testProducer) Plan() (*JobPlan, error) { return p.compute() }
