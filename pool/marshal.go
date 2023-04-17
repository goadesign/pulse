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
func marshalJob(job *Job) ([]byte, error) {
	var buf bytes.Buffer

	// Write Key length as int32
	err := binary.Write(&buf, binary.LittleEndian, int32(len(job.Key)))
	if err != nil {
		return nil, err
	}

	// Write Key bytes
	err = binary.Write(&buf, binary.LittleEndian, []byte(job.Key))
	if err != nil {
		return nil, err
	}

	// Write Payload length as int32
	err = binary.Write(&buf, binary.LittleEndian, int32(len(job.Payload)))
	if err != nil {
		return nil, err
	}

	// Write Payload bytes
	err = binary.Write(&buf, binary.LittleEndian, job.Payload)
	if err != nil {
		return nil, err
	}

	// Write CreatedAt as Unix timestamp in int64 format
	err = binary.Write(&buf, binary.LittleEndian, job.CreatedAt.UnixNano())
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// unmarshalJob unmarshals a job from a byte slice created by marshalJob.
func unmarshalJob(data []byte) (*Job, error) {
	reader := bytes.NewReader(data)

	// Read Key length as int32
	var keyLength int32
	err := binary.Read(reader, binary.LittleEndian, &keyLength)
	if err != nil {
		return nil, err
	}

	// Read Key bytes
	keyBytes := make([]byte, keyLength)
	err = binary.Read(reader, binary.LittleEndian, &keyBytes)
	if err != nil {
		return nil, err
	}
	keyValue := string(keyBytes)

	// Read Payload length as int32
	var payloadLength int32
	err = binary.Read(reader, binary.LittleEndian, &payloadLength)
	if err != nil {
		return nil, err
	}

	// Read Payload bytes
	var payload []byte
	if payloadLength > 0 {
		payload = make([]byte, payloadLength)
		err = binary.Read(reader, binary.LittleEndian, &payload)
		if err != nil {
			return nil, err
		}
	}

	// Read CreatedAt as Unix timestamp in int64 format
	var createdAtTimestamp int64
	err = binary.Read(reader, binary.LittleEndian, &createdAtTimestamp)
	if err != nil {
		return nil, err
	}
	createdAtValue := time.Unix(0, createdAtTimestamp).UTC()

	// Construct Job struct and return
	return &Job{
		Key:       keyValue,
		Payload:   payload,
		CreatedAt: createdAtValue,
	}, nil
}

func unmarshalJobKey(data []byte) (string, error) {
	reader := bytes.NewReader(data)

	// Read Key length as int32
	var keyLength int32
	err := binary.Read(reader, binary.LittleEndian, &keyLength)
	if err != nil {
		return "", err
	}

	// Read Key bytes
	keyBytes := make([]byte, keyLength)
	err = binary.Read(reader, binary.LittleEndian, &keyBytes)
	if err != nil {
		return "", err
	}
	return string(keyBytes), nil
}
