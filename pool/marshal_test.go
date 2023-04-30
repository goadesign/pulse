package pool

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/uuid"
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

func TestMarshalPoolWoker(t *testing.T) {
	// Test cases
	testCases := []struct {
		name   string
		worker poolWorker
	}{
		{
			name: "simple worker",
			worker: poolWorker{
				ID:          uuid.New(),
				CreatedAt:   time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
				RefreshedAt: time.Date(2022, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
			},
		},
		{
			name:   "empty worker",
			worker: poolWorker{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			marshaled := marshalWorker(&tc.worker)
			worker := unmarshalWorker(marshaled)

			// Compare original and unmarshaled poolWorker structs
			assert.Equal(t, tc.worker.ID, worker.ID)
			assert.Equal(t, tc.worker.CreatedAt, worker.CreatedAt)
			assert.Equal(t, tc.worker.RefreshedAt, worker.RefreshedAt)

			// Compare original and unmarshaled byte slices
			marshaled2 := marshalWorker(worker)
			assert.True(t, bytes.Equal(marshaled, marshaled2))
		})
	}
}

func TestMarshalPendingJob(t *testing.T) {
	// Test cases
	testCases := []struct {
		name string
		job  pendingJob
	}{
		{
			name: "simple job",
			job: pendingJob{
				Key:       "foo",
				CreatedAt: time.Date(2022, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
			},
		},
		{
			name: "done job",
			job: pendingJob{
				Key:       "foo",
				CreatedAt: time.Date(2022, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
				Done:      true,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			marshaled := marshalPendingJob(&tc.job)
			job := unmarshalPendingJob(marshaled)

			// Compare original and unmarshaled poolJob structs
			assert.Equal(t, tc.job.CreatedAt, job.CreatedAt)

			// Compare original and unmarshaled byte slices
			marshaled2 := marshalPendingJob(job)
			assert.Equal(t, marshaled, marshaled2)
		})
	}
}
