package pool

import (
	"bytes"
	"encoding/binary"
	"time"
)

// marshalWorker marshals w into a byte slice.
func marshalWorker(w *poolWorker) []byte {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, w); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// unmarshal unmarshals a poolWorker from a byte slice created by marshalWorker.
func unmarshalWorker(b []byte) *poolWorker {
	var w poolWorker
	if err := binary.Read(bytes.NewReader(b), binary.LittleEndian, &w); err != nil {
		panic(err)
	}
	return &w
}

// marshalJob marshals a job into a byte slice.
func marshalJob(job *Job) []byte {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(job.Key))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, []byte(job.Key)); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(job.Payload))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, job.Payload); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, job.CreatedAt.UnixNano()); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// unmarshalJob unmarshals a job from a byte slice created by marshalJob.
func unmarshalJob(data []byte) *Job {
	reader := bytes.NewReader(data)
	var keyLength int32
	if err := binary.Read(reader, binary.LittleEndian, &keyLength); err != nil {
		panic(err)
	}
	keyBytes := make([]byte, keyLength)
	if err := binary.Read(reader, binary.LittleEndian, &keyBytes); err != nil {
		panic(err)
	}
	var payloadLength int32
	if err := binary.Read(reader, binary.LittleEndian, &payloadLength); err != nil {
		panic(err)
	}
	var payload []byte
	if payloadLength > 0 {
		payload = make([]byte, payloadLength)
		if err := binary.Read(reader, binary.LittleEndian, &payload); err != nil {
			panic(err)
		}
	}
	var createdAtTimestamp int64
	if err := binary.Read(reader, binary.LittleEndian, &createdAtTimestamp); err != nil {
		panic(err)
	}
	return &Job{
		Key:       string(keyBytes),
		Payload:   payload,
		CreatedAt: time.Unix(0, createdAtTimestamp).UTC(),
	}
}

func unmarshalJobKey(data []byte) string {
	reader := bytes.NewReader(data)
	var keyLength int32
	if err := binary.Read(reader, binary.LittleEndian, &keyLength); err != nil {
		panic(err)
	}
	keyBytes := make([]byte, keyLength)
	if err := binary.Read(reader, binary.LittleEndian, &keyBytes); err != nil {
		panic(err)
	}
	return string(keyBytes)
}

// marshalPendingJob marshals j into a string.
func marshalPendingJob(job *pendingJob) string {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(job.Key))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, []byte(job.Key)); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, job.CreatedAt); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, job.Done); err != nil {
		panic(err)
	}
	return buf.String()
}

// unmarshalPendingJob unmarshals a poolJob from a byte slice created by marshalPoolJob.
func unmarshalPendingJob(s string) *pendingJob {
	var job pendingJob
	reader := bytes.NewReader([]byte(s))
	var keyLength int32
	if err := binary.Read(reader, binary.LittleEndian, &keyLength); err != nil {
		panic(err)
	}
	keyBytes := make([]byte, keyLength)
	if err := binary.Read(reader, binary.LittleEndian, &keyBytes); err != nil {
		panic(err)
	}
	job.Key = string(keyBytes)
	if err := binary.Read(reader, binary.LittleEndian, &job.CreatedAt); err != nil {
		panic(err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &job.Done); err != nil {
		panic(err)
	}
	return &job
}
