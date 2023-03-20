package streams

import (
	"context"

	redis "github.com/redis/go-redis/v9"
)

type (
	// Event is a stream event.
	Event struct {
		// ID is the unique event ID.
		ID string
		// StreamName is the name of the stream the event belongs to.
		StreamName string
		// SinkName is the name of the sink the event belongs to.
		SinkName string
		// EventName is the producer-defined event name.
		EventName string
		// Topic is the producer-defined event topic if any, empty string if none.
		Topic string
		// Payload is the event payload.
		Payload []byte
		// rdb is the redis client.
		rdb *redis.Client
	}
)

// newEvent creates a new event.
func newEvent(rdb *redis.Client, streamName, sinkName, id, eventName, topic string, payload []byte) *Event {
	return &Event{
		ID:         id,
		StreamName: streamName,
		SinkName:   sinkName,
		EventName:  eventName,
		Topic:      topic,
		Payload:    payload,
		rdb:        rdb,
	}
}

// Ack acknowledges the event.
func (e *Event) Ack(ctx context.Context) error {
	return e.rdb.XAck(ctx, e.StreamName, e.SinkName, e.ID).Err()
}
