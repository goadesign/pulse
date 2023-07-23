package pool

import (
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/pulse/rmap"
)

func TestSchedule(t *testing.T) {
	var (
		rdb      = redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: redisPwd})
		ctx, buf = testLogContext(t)
		testName = ulid.Make().String()
		node     = newTestNode(t, ctx, rdb, testName)
		worker   = newTestWorker(t, ctx, node)
		d        = 10 * time.Millisecond
		iter     = 0
	)
	defer cleanup(t, rdb, false, testName)

	producer := newTestProducer(testName, func() (*JobPlan, error) {
		iter++
		switch iter {
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
		t.Errorf("unexpected iteration %d", iter)
		return nil, nil
	})

	// Observe call to reset
	jobMap, err := rmap.Join(ctx, testName, rdb)
	require.NoError(t, err)
	var reset bool
	c := jobMap.Subscribe()
	defer jobMap.Unsubscribe(c)
	go func() {
		for ev := range c {
			if ev == rmap.EventReset {
				reset = true
				return
			}
		}
	}()

	err = node.Schedule(ctx, producer, d)
	require.NoError(t, err)

	jobMap.Subscribe()
	assert.Eventually(t, func() bool { return iter == 7 }, max, delay, "schedule should have stopped")
	assert.Eventually(t, func() bool { return reset }, max, delay, "job map should have been reset")
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
