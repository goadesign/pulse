package pool

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(job.NodeID))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, []byte(job.NodeID)); err != nil {
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
	if err := binary.Write(&buf, binary.LittleEndian, job.Requeued); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(job.dispatchID))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, []byte(job.dispatchID)); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// unmarshalJob decodes the current complete job payload. Mixed-version stream
// entries are rejected because pool upgrades require a quiescent boundary.
func unmarshalJob(data []byte) (*Job, error) {
	reader := bytes.NewReader(data)
	key, err := unmarshalString(reader, "job key")
	if err != nil {
		return nil, err
	}
	nodeID, err := unmarshalString(reader, "job node ID")
	if err != nil {
		return nil, err
	}
	payload, err := unmarshalBytes(reader, "job payload")
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		payload = nil
	}
	var createdAtTimestamp int64
	if err := binary.Read(reader, binary.LittleEndian, &createdAtTimestamp); err != nil {
		return nil, fmt.Errorf("decode job created-at: %w", err)
	}
	requeued, err := unmarshalBool(reader, "job requeued")
	if err != nil {
		return nil, err
	}
	dispatchID, err := unmarshalString(reader, "job dispatch ID")
	if err != nil {
		return nil, err
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("decode job: %d trailing bytes", reader.Len())
	}
	return &Job{
		Key:        key,
		Payload:    payload,
		CreatedAt:  time.Unix(0, createdAtTimestamp).UTC(),
		NodeID:     nodeID,
		Requeued:   requeued,
		dispatchID: dispatchID,
	}, nil
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

// unmarshalJobKey decodes one complete job-key payload.
func unmarshalJobKey(data []byte) (string, error) {
	reader := bytes.NewReader(data)
	key, err := unmarshalString(reader, "job key")
	if err != nil {
		return "", err
	}
	if reader.Len() != 0 {
		return "", fmt.Errorf("decode job key: %d trailing bytes", reader.Len())
	}
	return key, nil
}

// unmarshalBool decodes the exact binary bool representation.
func unmarshalBool(reader *bytes.Reader, field string) (bool, error) {
	value, err := reader.ReadByte()
	if err != nil {
		return false, fmt.Errorf("decode %s: %w", field, err)
	}
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("decode %s: invalid boolean value %d", field, value)
	}
}

// unmarshalString reads one validated length-prefixed string.
func unmarshalString(reader *bytes.Reader, field string) (string, error) {
	value, err := unmarshalBytes(reader, field)
	return string(value), err
}

// unmarshalBytes rejects negative, oversized, and truncated length-prefixed
// fields before allocating.
func unmarshalBytes(reader *bytes.Reader, field string) ([]byte, error) {
	var length int32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return nil, fmt.Errorf("decode %s length: %w", field, err)
	}
	if length < 0 {
		return nil, fmt.Errorf("decode %s: negative length %d", field, length)
	}
	if int64(length) > int64(reader.Len()) {
		return nil, fmt.Errorf("decode %s: length %d exceeds remaining %d bytes", field, length, reader.Len())
	}
	value := make([]byte, length)
	if err := binary.Read(reader, binary.LittleEndian, &value); err != nil {
		return nil, fmt.Errorf("decode %s: %w", field, err)
	}
	return value, nil
}

// marshalKeyedPayload marshals the shared wire shape used by events whose
// routing key is distinct from their opaque handler payload.
func marshalKeyedPayload(key string, payload []byte) []byte {
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

// unmarshalKeyedPayload decodes one complete keyed payload.
func unmarshalKeyedPayload(data []byte) (string, []byte, error) {
	reader := bytes.NewReader(data)
	key, err := unmarshalString(reader, "keyed payload key")
	if err != nil {
		return "", nil, err
	}
	payload, err := unmarshalBytes(reader, "keyed payload")
	if err != nil {
		return "", nil, err
	}
	if reader.Len() != 0 {
		return "", nil, fmt.Errorf("decode keyed payload: %d trailing bytes", reader.Len())
	}
	return key, payload, nil
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

// unmarshalEnvelope decodes one complete sender envelope.
func unmarshalEnvelope(data []byte) (string, []byte, error) {
	reader := bytes.NewReader(data)
	sender, err := unmarshalString(reader, "envelope sender")
	if err != nil {
		return "", nil, err
	}
	payload, err := unmarshalBytes(reader, "envelope payload")
	if err != nil {
		return "", nil, err
	}
	if reader.Len() != 0 {
		return "", nil, fmt.Errorf("decode envelope: %d trailing bytes", reader.Len())
	}
	return sender, payload, nil
}

// marshalAck marshals an ack into a byte slice.
func marshalAck(ak *ack) []byte {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(ak.EventID))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, []byte(ak.EventID)); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(ak.Error))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, []byte(ak.Error)); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(ak.JobKey))); err != nil {
		panic(err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, []byte(ak.JobKey)); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// unmarshalAck decodes the current complete acknowledgement. Mixed wire
// versions are rejected by the mandatory quiescent-upgrade contract.
func unmarshalAck(data []byte) (*ack, error) {
	reader := bytes.NewReader(data)
	eventID, err := unmarshalString(reader, "ack event ID")
	if err != nil {
		return nil, err
	}
	errorMessage, err := unmarshalString(reader, "ack error")
	if err != nil {
		return nil, err
	}
	jobKey, err := unmarshalString(reader, "ack job key")
	if err != nil {
		return nil, err
	}
	result := &ack{
		EventID: eventID,
		Error:   errorMessage,
		JobKey:  jobKey,
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("decode acknowledgement: %d trailing bytes", reader.Len())
	}
	return result, nil
}
