package pool

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{
			name: "requeued job",
			job: Job{
				Key:       "test-key",
				Payload:   []byte("test-payload"),
				CreatedAt: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
				Requeued:  true,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			marshaled := marshalJob(&tc.job)
			job, err := unmarshalJob(marshaled)
			require.NoError(t, err)

			// Compare original and unmarshaled Job structs
			assert.Equal(t, tc.job.Key, job.Key)
			assert.Equal(t, tc.job.Payload, job.Payload)
			assert.Equal(t, tc.job.CreatedAt, job.CreatedAt)
			assert.Equal(t, tc.job.Requeued, job.Requeued)

			// Compare original and unmarshaled byte slices
			marshaled2 := marshalJob(job)
			assert.True(t, bytes.Equal(marshaled, marshaled2))

			// Compare unmarshaled job key
			key, err := unmarshalJobKey(marshalJobKey(tc.job.Key))
			require.NoError(t, err)
			assert.Equal(t, tc.job.Key, key)
		})
	}
}

func TestUnmarshalJobRejectsLegacyFormats(t *testing.T) {
	job := &Job{
		Key:       "test-key",
		Payload:   []byte("test-payload"),
		CreatedAt: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
		NodeID:    "test-node",
		Requeued:  true,
	}
	marshaled := marshalJob(job)
	legacy := marshaled[:len(marshaled)-5]

	_, err := unmarshalJob(legacy)
	require.Error(t, err)
	_, err = unmarshalJob(marshaled[:len(marshaled)-4])
	require.Error(t, err)
}

func TestMarshalKeyedPayload(t *testing.T) {
	key := "test-key"
	payload := []byte("test-payload")

	marshaled := marshalKeyedPayload(key, payload)
	gotKey, gotPayload, err := unmarshalKeyedPayload(marshaled)
	require.NoError(t, err)

	assert.Equal(t, key, gotKey)
	assert.Equal(t, payload, gotPayload)
	decodedKey, err := unmarshalJobKey(marshalJobKey(key))
	require.NoError(t, err)
	assert.Equal(t, key, decodedKey)
}

func TestPoolDecodersRejectMalformedPayloads(t *testing.T) {
	job := marshalJob(&Job{Key: "job", CreatedAt: time.Unix(1, 0)})
	jobKey := marshalJobKey("job")
	keyed := marshalKeyedPayload("job", []byte("payload"))
	envelope := marshalEnvelope("node", []byte("payload"))
	ackPayload := marshalAck(&ack{EventID: "event", JobKey: "job"})

	cases := []struct {
		name   string
		decode func([]byte) error
		data   []byte
	}{
		{
			name: "job truncated",
			decode: func(data []byte) error {
				_, err := unmarshalJob(data)
				return err
			},
			data: job[:len(job)-1],
		},
		{
			name: "job trailing",
			decode: func(data []byte) error {
				_, err := unmarshalJob(data)
				return err
			},
			data: append(job, 1),
		},
		{
			name: "job key negative length",
			decode: func(data []byte) error {
				_, err := unmarshalJobKey(data)
				return err
			},
			data: []byte{0xff, 0xff, 0xff, 0xff},
		},
		{
			name: "job key trailing",
			decode: func(data []byte) error {
				_, err := unmarshalJobKey(data)
				return err
			},
			data: append(jobKey, 1),
		},
		{
			name: "keyed payload truncated",
			decode: func(data []byte) error {
				_, _, err := unmarshalKeyedPayload(data)
				return err
			},
			data: keyed[:len(keyed)-1],
		},
		{
			name: "envelope trailing",
			decode: func(data []byte) error {
				_, _, err := unmarshalEnvelope(data)
				return err
			},
			data: append(envelope, 1),
		},
		{
			name: "ack truncated",
			decode: func(data []byte) error {
				_, err := unmarshalAck(data)
				return err
			},
			data: ackPayload[:len(ackPayload)-1],
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.decode(tc.data))
		})
	}
}
