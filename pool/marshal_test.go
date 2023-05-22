package pool

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMarshalJob(t *testing.T) {
	testCases := []struct {
		name string
		job  Job
	}{
		{
			name: "simple job",
			job: Job{
				Key:       "test-key",
				Payload:   []byte("test-payload"),
				CreatedAt: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "empty job",
			job: Job{
				Key:       "test-key",
				CreatedAt: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			marshaled := marshalJob(&tc.job)
			job := unmarshalJob(marshaled)

			// Compare original and unmarshaled Job structs
			assert.Equal(t, tc.job.Key, job.Key)
			assert.Equal(t, tc.job.Payload, job.Payload)
			assert.Equal(t, tc.job.CreatedAt, job.CreatedAt)

			// Compare original and unmarshaled byte slices
			marshaled2 := marshalJob(job)
			assert.True(t, bytes.Equal(marshaled, marshaled2))

			// Compare unmarshaled job key
			key := unmarshalJobKey(marshaled)
			assert.Equal(t, tc.job.Key, key)
		})
	}
}
