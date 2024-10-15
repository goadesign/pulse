package pool

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/clue/log"
	"goa.design/pulse/pulse"

	ptesting "goa.design/pulse/testing"
)

func TestLogging(t *testing.T) {
	var buf bytes.Buffer
	ctx := log.Context(context.Background(), log.WithOutput(&buf), log.WithFormat(testFormat))
	log.FlushAndDisableBuffering(ctx)
	logger := pulse.ClueLogger(ctx)
	rdb := ptesting.NewRedisClient(t)
	node, err := AddNode(ctx, "test", rdb,
		WithLogger(logger),
		WithWorkerShutdownTTL(testWorkerShutdownTTL),
		WithJobSinkBlockDuration(testJobSinkBlockDuration),
		WithWorkerTTL(testWorkerTTL),
		WithAckGracePeriod(testAckGracePeriod))

	require.NoError(t, err)
	assert.NoError(t, node.Close(ctx))

	lines := strings.Split(buf.String(), "\n")
	lines = lines[:len(lines)-1] // Remove the last empty line

	require.Equal(t, len(expectedLogPatterns), len(lines), "Number of log lines doesn't match expected")

	for i, pattern := range expectedLogPatterns {
		assert.Regexp(t, pattern, lines[i], "Log line %d doesn't match expected pattern", i+1)
	}
}

func testFormat(e *log.Entry) []byte {
	var b bytes.Buffer
	for i, kv := range e.KeyVals {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(kv.K)
		b.WriteString("=")
		b.WriteString(fmt.Sprintf("%v", kv.V))
	}
	b.WriteString("\n")
	return b.Bytes()
}

var expectedLogPatterns = []string{
	`^pool=test node=[^ ]+ msg=options client_only=false max_queued_jobs=1000 worker_ttl=50ms worker_shutdown_ttl=100ms pending_job_ttl=20s job_sink_block_duration=50ms ack_grace_period=20ms$`,
	`^pool=test node=[^ ]+ map=test:shutdown msg=joined$`,
	`^pool=test node=[^ ]+ map=test:workers msg=joined$`,
	`^pool=test node=[^ ]+ msg=joined workers=\[\]$`,
	`^pool=test node=[^ ]+ map=test:jobs msg=joined$`,
	`^pool=test node=[^ ]+ map=test:job-payloads msg=joined$`,
	`^pool=test node=[^ ]+ map=test:keepalive msg=joined$`,
	`^pool=test node=[^ ]+ map=test:tickers msg=joined$`,
	`^pool=test node=[^ ]+ stream=test:pool map=stream:test:pool:sinks msg=joined$`,
	`^pool=test node=[^ ]+ stream=test:pool map=sink:events:keepalive msg=joined$`,
	`^pool=test node=[^ ]+ stream=test:pool msg=create sink=events start=\$$`,
	`^pool=test node=[^ ]+ stream=test:node:[^ ]+ msg=create reader start=0$`,
	`^pool=test node=[^ ]+ msg=closing$`,
	`^pool=test node=[^ ]+ stream=test:pool map=sink:events:keepalive msg=stopped$`,
	`^pool=test node=[^ ]+ stream=test:pool map=sink:events:keepalive msg=stopped$`,
	`^pool=test node=[^ ]+ sink=events consumer=[^ ]+ msg=closed$`,
	`^pool=test node=[^ ]+ map=test:tickers msg=stopped$`,
	`^pool=test node=[^ ]+ map=test:keepalive msg=stopped$`,
	`^pool=test node=[^ ]+ reader=test:node:[^ ]+ msg=stopped$`,
	`^pool=test node=[^ ]+ stream=test:node:[^ ]+ msg=stream deleted$`,
	`^(pool=test node=[^ ]+ map=test:workers msg=stopped|pool=test node=[^ ]+ map=test:shutdown msg=stopped)$`,
	`^(pool=test node=[^ ]+ map=test:workers msg=stopped|pool=test node=[^ ]+ map=test:shutdown msg=stopped)$`,
	`^pool=test node=[^ ]+ msg=closed$`,
}
