package pulse

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"goa.design/clue/log"

	ptesting "goa.design/pulse/testing"
)

func TestWithMultipleDerivedPulseLoggers(t *testing.T) {
	var buf ptesting.Buffer
	ctx := log.Context(context.Background(), log.WithOutput(&buf), log.WithFormat(log.FormatJSON))
	log.FlushAndDisableBuffering(ctx)
	logger := ClueLogger(ctx)

	logger.Info("root")

	logger1 := logger.WithPrefix("key1", "value1")
	logger1.Info("first")

	logger2 := logger.WithPrefix("key2", "value2")
	logger2.Info("second")
	logger1.Info("third")

	logger3 := logger1.WithPrefix("key3", "value3")
	logger3.Info("fourth")
	logger2.Info("fifth")
	logger1.Info("sixth")
	logger.Info("seventh")

	logs := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, logs, 8)

	// Helper function to check log entries
	checkLog := func(t *testing.T, log string, expectedLog string) {
		t.Helper()
		var logMap map[string]interface{}
		err := json.Unmarshal([]byte(log), &logMap)
		require.NoError(t, err, "Failed to parse JSON log entry")

		// Remove the time field
		delete(logMap, "time")

		// Convert back to JSON
		actualJSON, err := json.Marshal(logMap)
		require.NoError(t, err, "Failed to marshal log entry")

		assert.Equal(t, expectedLog, string(actualJSON), "Log entry doesn't match expected value")
	}

	checkLog(t, logs[0], `{"level":"info","msg":"root"}`)
	checkLog(t, logs[1], `{"key1":"value1","level":"info","msg":"first"}`)
	checkLog(t, logs[2], `{"key2":"value2","level":"info","msg":"second"}`)
	checkLog(t, logs[3], `{"key1":"value1","level":"info","msg":"third"}`)
	checkLog(t, logs[4], `{"key1":"value1","key3":"value3","level":"info","msg":"fourth"}`)
	checkLog(t, logs[5], `{"key2":"value2","level":"info","msg":"fifth"}`)
	checkLog(t, logs[6], `{"key1":"value1","level":"info","msg":"sixth"}`)
	checkLog(t, logs[7], `{"level":"info","msg":"seventh"}`)
}
