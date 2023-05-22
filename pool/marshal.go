package pool

import (
	"bytes"
	"encoding/binary"
	"time"
)

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

// marshalJobKey marshals a job key into a byte slice.
func marshalJobKey(key string) []byte {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(key))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, []byte(key)); err != nil {
		panic(err)
	}
	return buf.Bytes()
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

func marshalNotification(key string, payload []byte) []byte {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(key))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, []byte(key)); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(payload))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, payload); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func unmarshalNotification(data []byte) (string, []byte) {
	reader := bytes.NewReader(data)
	var keyLength int32
	if err := binary.Read(reader, binary.LittleEndian, &keyLength); err != nil {
		panic(err)
	}
	keyBytes := make([]byte, keyLength)
	if err := binary.Read(reader, binary.LittleEndian, &keyBytes); err != nil {
		panic(err)
	}
	// read payload
	var payloadLength int32
	if err := binary.Read(reader, binary.LittleEndian, &payloadLength); err != nil {
		panic(err)
	}
	payload := make([]byte, payloadLength)
	if err := binary.Read(reader, binary.LittleEndian, &payload); err != nil {
		panic(err)
	}
	return string(keyBytes), payload
}

// Envelope used to identify event sender.
func marshalEnvelope(sender string, payload []byte) []byte {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(sender))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, []byte(sender)); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(payload))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, payload); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// unmarshalEnvelope unmarshals an envelope from a byte slice created by marshalEnvelope.
func unmarshalEnvelope(data []byte) (string, []byte) {
	reader := bytes.NewReader(data)
	var senderLength int32
	if err := binary.Read(reader, binary.LittleEndian, &senderLength); err != nil {
		panic(err)
	}
	senderBytes := make([]byte, senderLength)
	if err := binary.Read(reader, binary.LittleEndian, &senderBytes); err != nil {
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
	return string(senderBytes), payload
}
